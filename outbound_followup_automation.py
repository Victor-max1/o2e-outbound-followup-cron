"""Automation du funnel de suivi "Outbound - Instantly" (liste outbound_people).

Reproduit le plan valide avec l'utilisateur : un seul passage stateless (pas de
wait multi-jours), qui recalcule l'etat a partir de `active_from` du statut
`Interest Status` courant (horodatage natif Attio, pas besoin de champ dedie).

  Replied - to follow-up
    -> email outbound d'Elliot (options2exit.com / state17.com) trouve
       apres l'entree dans ce statut ?  oui -> Follow-up: email sent

  Follow-up: email sent
    -> email inbound du prospect trouve apres notre envoi ? oui -> Connected
    -> sinon, >= 1 jour dans ce statut :
         mobile_phone deja rempli -> Follow-up: Aircall
         mobile_phone vide -> enrich_phone = "To Enrich", wait 60s, recheck :
           rempli -> Follow-up: Aircall
           toujours vide -> Auto-enrolled in sequence

  Follow-up: Aircall
    -> note Aircall "connectee" (duree > 10s) trouvee apres l'entree dans ce
       statut ? oui -> Connected
    -> sinon, >= 2 jours dans ce statut -> Auto-enrolled in sequence

  Auto-enrolled in sequence
    -> email inbound du prospect trouve apres notre envoi ? oui -> Connected

Detection "email inbound du prospect" : n'importe quel inbound depuis son
adresse apres l'horodatage de reference (pas besoin du meme thread, valide
avec l'utilisateur).

Dry-run par defaut (out/outbound_followup_preview.csv). --apply pour ecrire.
Concu pour tourner sur un cron/Task Scheduler toutes les 30 min (aucun etat
local requis, tout est relu depuis Attio a chaque run).
"""
import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(HERE, 'out')
os.makedirs(OUT, exist_ok=True)
load_dotenv(os.path.join(ROOT, '.env'))

API_KEY = os.getenv('ATTIO_API_KEY')
BASE = 'https://api.attio.com/v2'
HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}
LIST_OUTBOUND_PEOPLE = 'outbound_people'

ELLIOT_EMAILS = {'ejarvis@options2exit.com', 'ejarvis@state17.com'}
ONE_DAY_S = 24 * 60 * 60
TWO_DAYS_S = 2 * 24 * 60 * 60
ONE_WEEK_S = 7 * 24 * 60 * 60
CALL_CONNECTED_MIN_SECONDS = 10

DURATION_RE = re.compile(r'Call lasted (\d+) seconds')


def call(method, path, payload=None, retries=5):
    data = json.dumps(payload).encode() if payload is not None else None
    for attempt in range(retries):
        req = urllib.request.Request(BASE + path, data=data, headers=HEADERS, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:500]
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            return {'__err': f'{e.code} {body}'}
        except urllib.error.URLError:
            time.sleep(2 ** attempt)
    return {'__err': 'retries exhausted'}


def parse_ts(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def fetch_all_entries(list_slug, limit=500):
    out, offset = [], 0
    while True:
        res = call('POST', f'/lists/{list_slug}/entries/query', {'limit': limit, 'offset': offset})
        if '__err' in res:
            print(f'   ERR {list_slug}: {res["__err"]}')
            break
        batch = res.get('data', [])
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return out


def get_status(entry_values):
    arr = entry_values.get('interest_status') or []
    if not arr:
        return None, None
    return arr[0]['status']['title'], parse_ts(arr[0]['active_from'])


def get_person(record_id):
    res = call('GET', f'/objects/people/records/{record_id}')
    if '__err' in res:
        return None
    return res['data']['values']


def person_emails(values):
    return [e['email_address'] for e in (values.get('email_addresses') or [])]


def person_mobile(values):
    arr = values.get('mobile_phone') or []
    return arr[0].get('value', '') if arr else ''


def list_emails_for_person(person_id):
    """Toutes les emails liees a cette personne (endpoint GET /v2/emails)."""
    out, cursor = [], None
    while True:
        qs = f'linked_object=people&linked_record_ids={person_id}&limit=50'
        if cursor:
            qs += f'&cursor={cursor}'
        res = call('GET', f'/emails?{qs}')
        if '__err' in res:
            return out, res['__err']
        out.extend(res.get('data', []))
        cursor = (res.get('pagination') or {}).get('next_cursor')
        if not cursor:
            break
    return out, None


def has_outbound_from_elliot_after(emails, after_dt):
    for e in emails:
        if e.get('direction') != 'outbound':
            continue
        sent_at = parse_ts(e.get('sent_at'))
        if not sent_at or sent_at <= after_dt:
            continue
        senders = {p['email_address'] for p in e.get('participants', []) if p.get('role') == 'from'}
        if senders & ELLIOT_EMAILS:
            return True, sent_at
    return False, None


def has_inbound_from_prospect_after(emails, prospect_emails, after_dt):
    prospect_emails = {e.lower() for e in prospect_emails}
    for e in emails:
        if e.get('direction') != 'inbound':
            continue
        sent_at = parse_ts(e.get('sent_at'))
        if not sent_at or sent_at <= after_dt:
            continue
        senders = {p['email_address'].lower() for p in e.get('participants', []) if p.get('role') == 'from'}
        if senders & prospect_emails:
            return True, sent_at
    return False, None


def get_notes_for_person(person_id):
    res = call('GET', f'/notes?parent_object=people&parent_record_id={person_id}&limit=50')
    if '__err' in res:
        return [], res['__err']
    return res.get('data', []), None


def has_connected_call_after(notes, after_dt):
    for n in notes:
        created = parse_ts(n.get('created_at'))
        if not created or created <= after_dt:
            continue
        m = DURATION_RE.search(n.get('content_plaintext') or '')
        if m and int(m.group(1)) > CALL_CONNECTED_MIN_SECONDS:
            return True, created
    return False, None


def process_entry(entry, now):
    entry_id = entry['id']['entry_id']
    person_id = entry['parent_record_id']
    status, active_from = get_status(entry['entry_values'])
    if not status or not active_from:
        return None

    row = {'entry_id': entry_id, 'person_id': person_id, 'cur_status': status,
           'active_from': active_from.isoformat(), 'target_status': '',
           'extra_writes': '', 'reason': ''}

    age_s = (now - active_from).total_seconds()

    # Un call connecte compte peu importe l'etape ou l'entree se trouve
    # actuellement -- Elliot peut appeler pendant que le statut est encore
    # "Replied" ou "Follow-up: email sent", pas seulement une fois arrive a
    # "Follow-up: Aircall". Reference temporelle = creation de l'entree dans
    # la liste (stable, couvre tout le cycle de vie du prospect dans ce
    # funnel), pas l'active_from du statut courant qui bougerait a chaque
    # transition et raterait des calls anterieurs.
    entry_created = parse_ts(entry.get('created_at'))
    if entry_created:
        notes, err = get_notes_for_person(person_id)
        if not err:
            connected, created = has_connected_call_after(notes, entry_created)
            if connected:
                row['target_status'] = 'Connected'
                row['reason'] = f'call connecte (detecte hors etape Aircall) @ {created.isoformat()}'
                return row

    if status == 'Replied - to follow-up':
        if age_s > ONE_WEEK_S:
            row['reason'] = f'reponse trop ancienne ({age_s/86400:.0f}j) -- hors scope'
            return row
        emails, err = list_emails_for_person(person_id)
        if err:
            row['reason'] = f'email fetch err: {err}'
            return row
        found, sent_at = has_outbound_from_elliot_after(emails, active_from)
        if found:
            row['target_status'] = 'Follow-up: email sent'
            row['reason'] = f'Elliot outbound @ {sent_at.isoformat()}'
        return row

    if status == 'Follow-up: email sent':
        person = get_person(person_id)
        if person is None:
            row['reason'] = 'person fetch err'
            return row
        prospect_emails = person_emails(person)
        emails, err = list_emails_for_person(person_id)
        if err:
            row['reason'] = f'email fetch err: {err}'
            return row
        replied, sent_at = has_inbound_from_prospect_after(emails, prospect_emails, active_from)
        if replied:
            row['target_status'] = 'Connected'
            row['reason'] = f'prospect replied @ {sent_at.isoformat()}'
            return row
        if age_s >= ONE_DAY_S:
            mobile = person_mobile(person)
            if mobile:
                row['target_status'] = 'Follow-up: Aircall'
                row['reason'] = '1j+ sans reponse, mobile_phone deja rempli'
            else:
                row['target_status'] = 'PENDING_ENRICH'
                row['reason'] = '1j+ sans reponse, mobile_phone vide -> to enrich + wait 60s'
        return row

    if status == 'Follow-up: Aircall':
        # Le call connecte est deja couvert par le check universel ci-dessus ;
        # ici on ne gere que le fallback "pas de connexion apres 2 jours".
        if age_s >= TWO_DAYS_S:
            row['target_status'] = 'Auto-enrolled in sequence'
            row['reason'] = '2j+ sans call connecte'
        return row

    if status == 'Auto-enrolled in sequence':
        person = get_person(person_id)
        if person is None:
            row['reason'] = 'person fetch err'
            return row
        prospect_emails = person_emails(person)
        emails, err = list_emails_for_person(person_id)
        if err:
            row['reason'] = f'email fetch err: {err}'
            return row
        replied, sent_at = has_inbound_from_prospect_after(emails, prospect_emails, active_from)
        if replied:
            row['target_status'] = 'Connected'
            row['reason'] = f'prospect replied @ {sent_at.isoformat()}'
        return row

    return None  # statut hors scope de cette automation (Connected, Not interested, etc.)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    now = datetime.now(timezone.utc)

    print('1/2 Entrees outbound_people (statuts dans le scope de l\'automation)...')
    entries = fetch_all_entries(LIST_OUTBOUND_PEOPLE)
    in_scope_statuses = {'Replied - to follow-up', 'Follow-up: email sent',
                          'Follow-up: Aircall', 'Auto-enrolled in sequence'}
    scoped = []
    for e in entries:
        status, _ = get_status(e['entry_values'])
        if status in in_scope_statuses:
            scoped.append(e)
    print(f'   {len(entries)} entrees au total, {len(scoped)} dans un statut suivi')

    print('2/2 Traitement par entree...')
    results = []
    for i, e in enumerate(scoped, 1):
        r = process_entry(e, now)
        if r:
            results.append(r)
        if i % 25 == 0 or i == len(scoped):
            print(f'   ...{i}/{len(scoped)}', end='\r', flush=True)
    print(' ' * 40, end='\r')

    preview_path = os.path.join(OUT, 'outbound_followup_preview.csv')
    with open(preview_path, 'w', encoding='utf-8', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=['entry_id', 'person_id', 'cur_status', 'active_from',
                                            'target_status', 'extra_writes', 'reason'])
        w.writeheader()
        w.writerows(results)

    by_transition = Counter((r['cur_status'], r['target_status']) for r in results if r['target_status'])
    pending_enrich = [r for r in results if r['target_status'] == 'PENDING_ENRICH']

    print('=' * 70)
    print(f'{len(results)} entrees analysees -> {preview_path}')
    for (cur, tgt), n in by_transition.most_common():
        print(f'   {cur:28} -> {tgt:28} {n}')
    print(f'   dont {len(pending_enrich)} necessitent le cycle enrich_phone (To Enrich + wait 60s)')
    print('=' * 70)

    if not args.apply:
        print("\nDRY-RUN : rien n'a ete ecrit. Relancer avec --apply pour appliquer.")
        return

    print('\nEcriture...')
    ok, errors = 0, []
    to_write = [r for r in results if r['target_status']]
    print(f'{len(to_write)} necessitent une ecriture (sur {len(results)} analysees)')
    for r in to_write:
        if r['target_status'] == 'PENDING_ENRICH':
            pres = call('PATCH', f"/objects/people/records/{r['person_id']}",
                         {'data': {'values': {'enrich_phone': ['To Enrich']}}})
            if '__err' in pres:
                errors.append((r['entry_id'], f"enrich_phone: {pres['__err']}"))
                continue
            time.sleep(60)
            person = get_person(r['person_id'])
            mobile = person_mobile(person) if person else ''
            final_status = 'Follow-up: Aircall' if mobile else 'Auto-enrolled in sequence'
        else:
            final_status = r['target_status']

        res = call('PATCH', f"/lists/{LIST_OUTBOUND_PEOPLE}/entries/{r['entry_id']}",
                    {'data': {'entry_values': {'interest_status': [final_status]}}})
        if '__err' in res:
            errors.append((r['entry_id'], res['__err']))
        else:
            ok += 1
    print(f'mis a jour : {ok}   erreurs : {len(errors)}')
    for eid, msg in errors[:15]:
        print(f'   {eid} -> {str(msg)[:200]}')


if __name__ == '__main__':
    main()
