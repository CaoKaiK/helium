import os
from datetime import date, datetime
import calendar
import openpyxl
import pandas as pd


def convert_df_to_cointracking(wallet_df, export_path):
  cointracking_col = [
  'Type',
  'Buy Amount',
  'Buy Currency',
  'Sell Amount',
  'Sell Currency',
  'Fee',
  'Fee Currency',
  'Exchange',
  'Trade Group',
  'Comment',
  'Date'
  ]

  cointracking_col_ext = cointracking_col.copy()
  cointracking_col_ext.extend([
    'Tx-ID',
    'Buy Value in your Account Currency',
    'Sell Value in your Account Currency'
    ])

  # add fee, fee_hnt, fee_usd if not existing
  if 'fee' not in wallet_df.columns:
    wallet_df['fee'] = ''
    wallet_df['fee_hnt'] = ''
    wallet_df['fee_usd'] = ''
  
  # Type
  wallet_df['type'] = wallet_df['type'].str.capitalize()
  wallet_df['type'] = wallet_df['type'].replace('Payment', 'Withdrawal')

  cointracking_df = pd.DataFrame(columns=cointracking_col)
  cointracking_df_ext =pd.DataFrame(columns=cointracking_col_ext)

  for index, row in wallet_df.iterrows():
    height = row['height']

    if row['type'] == 'Mining':
      entry = {
        'Type': row['type'],
        'Buy Amount': row['amount'],
        'Buy Currency': 'HNT2',
        'Sell Amount': '',
        'Sell Currency': '',
        'Fee': '',
        'Fee Currency': '',
        'Exchange': 'Helium',
        'Trade Group': '',
        'Comment': f'Block {height}',
        'Date': row['time']
      }

      entry_extended = entry.copy()
      entry_extended.update({
        'Tx-ID': '',
        'Buy Value in your Account Currency': row['price'] * row['amount'],
        'Sell Value in your Account Currency': ''
      })
    elif row['type'] == 'Withdrawal':
      entry = {
        'Type': row['type'],
        'Buy Amount': '',
        'Buy Currency': '',
        'Sell Amount': row['amount'],
        'Sell Currency': 'HNT2',
        'Fee': row['fee_hnt'],
        'Fee Currency': 'HNT2',
        'Exchange': 'Helium',
        'Trade Group': '',
        'Comment': f'Block {height}',
        'Date': row['time']
      }
      entry_extended = entry.copy()
      entry_extended.update({
        'Tx-ID': '',
        'Buy Value in your Account Currency': '',
        'Sell Value in your Account Currency': row['price'] * row['amount']
      })
    else:
      type_e = row['type']
      print(f'Error in Export {type_e}')
    
    cointracking_df = cointracking_df.append(entry, ignore_index=True)
    cointracking_df_ext = cointracking_df_ext.append(entry_extended, ignore_index=True)
  
  file_path = export_path + '.csv'
  cointracking_df.to_csv(file_path, index=False)
  with open(file_path, 'r') as csv_file:
    data = csv_file.read()
  with open(file_path, 'w') as csv_file:
    csv_file.write(data[:-1])

  file_path = export_path + '_ext.csv'
  cointracking_df_ext.to_csv(file_path, index=False)
  with open(file_path, 'r') as csv_file:
    data = csv_file.read()
  with open(file_path, 'w') as csv_file:
    csv_file.write(data[:-1])

  return None

def convert_df_to_datev(wallet_df, export_path):
  # columns for datev export
  datev_col = [
    'Umsatz (ohne Soll/Haben-Kz)',
    'Soll/Haben-Kennzeichen',
    'WKZ',
    'Umsatz',
    'Kurs',
    'Basis-Umsatz',
    'WKZ Basis-Umsatz',
    'Konto',
    'Gegenkonto (ohne BU-Schlüssel)',
    'BU-Schlüssel',
    'Belegdatum',
    'Belegfeld 1',
    'Belegfeld 2',
    'Skonto',
    'Buchungstext',
    'Postensperre',
    'Diverse Adressnummer',
    'Geschäftspartnerbank',
    'Sachverhalt',
    'Zinssperre',
    'Beleglink',
    'Beleginfo - Art 1',
    'Beleginfo - Inhalt 1',
    'Beleginfo - Art 2',
    'Beleginfo - Inhalt 2',
    'Beleginfo - Art 3',
    'Beleginfo - Inhalt 3',
    'Beleginfo - Art 4',
    'Beleginfo - Inhalt 4',
    'Beleginfo - Art 5',
    'Beleginfo - Inhalt 5',
    'Beleginfo - Art 6',
    'Beleginfo - Inhalt 6',
    'Beleginfo - Art 7',
    'Beleginfo - Inhalt 7',
    'Beleginfo - Art 8',
    'Beleginfo - Inhalt 8',
    'KOST1 - Kostenstelle',
    'KOST2 - Kostenstelle',
    'Kost-Menge',
    'EU-Land u. UStID',
    'EU-Steuersatz',
    'Abw. Versteuerungsart',
    'Sachverhalt L+L',
    'Funktionsergänzung L+L',
    'BU 49 Hauptfunktionstyp',
    'BU 49 Hauptfunktionsnummer',
    'BU 49 Funktionsergänzung',
    'Zusatzinformation - Art 1',
    'Zusatzinformation- Inhalt 1',
    'Zusatzinformation - Art 2',
    'Zusatzinformation- Inhalt 2',
    'Zusatzinformation - Art 3',
    'Zusatzinformation- Inhalt 3',
    'Zusatzinformation - Art 4',
    'Zusatzinformation- Inhalt 4',
    'Zusatzinformation - Art 5',
    'Zusatzinformation- Inhalt 5',
    'Zusatzinformation - Art 6',
    'Zusatzinformation- Inhalt 6',
    'Zusatzinformation - Art 7',
    'Zusatzinformation- Inhalt 7',
    'Zusatzinformation - Art 8',
    'Zusatzinformation- Inhalt 8',
    'Zusatzinformation - Art 9',
    'Zusatzinformation- Inhalt 9',
    'Zusatzinformation - Art 10',
    'Zusatzinformation- Inhalt 10',
    'Zusatzinformation - Art 11',
    'Zusatzinformation- Inhalt 11',
    'Zusatzinformation - Art 12',
    'Zusatzinformation- Inhalt 12',
    'Zusatzinformation - Art 13',
    'Zusatzinformation- Inhalt 13',
    'Zusatzinformation - Art 14',
    'Zusatzinformation- Inhalt 14',
    'Zusatzinformation - Art 15',
    'Zusatzinformation- Inhalt 15',
    'Zusatzinformation - Art 16',
    'Zusatzinformation- Inhalt 16',
    'Zusatzinformation - Art 17',
    'Zusatzinformation- Inhalt 17',
    'Zusatzinformation - Art 18',
    'Zusatzinformation- Inhalt 18',
    'Zusatzinformation - Art 19',
    'Zusatzinformation- Inhalt 19',
    'Zusatzinformation - Art 20',
    'Zusatzinformation- Inhalt 20',
    'Stück',
    'Gewicht',
    'Zahlweise',
    'Forderungsart',
    'Veranlagungsjahr',
    'Zugeordnete Fälligkeit',
    'Skontotyp',
    'Auftragsnummer',
    'Buchungstyp',
    'USt-Schlüssel (Anzahlungen)',
    'EU-Land (Anzahlungen)',
    'Sachverhalt L+L (Anzahlungen)',
    'EU-Steuersatz (Anzahlungen)',
    'Erlöskonto (Anzahlungen)',
    'Herkunft-Kz',
    'Buchungs GUID',
    'KOST-Datum',
    'SEPA-Mandatsreferenz',
    'Skontosperre',
    'Gesellschaftername',
    'Beteiligtennummer',
    'Identifikationsnummer',
    'Zeichnernummer',
    'Postensperre bis',
    'Bezeichnung SoBil-Sachverhalt',
    'Kennzeichen SoBil-Buchung',
    'Festschreibung',
    'Leistungsdatum',
    'Datum Zuord. Steuerperiode',
    'Fälligkeit',
    'Generalumkehr',
    'Steuersatz',
    'Land'
  ]
  # empty entry dict
  entry = dict.fromkeys(datev_col, '')
  datev_df = pd.DataFrame(columns=datev_col)
  
  # monthly correction for amounts below cents
  running_balance = 0

  # iterate events in wallet
  for index, row in wallet_df.iterrows():
    height = row['height']
    year = row['year']
    month = row['month']
    day = row['day']
    datum = int(str(day) + str(month).zfill(2))
    
    # mining events
    if row['type'] == 'mining':
      entry_mining = entry.copy()
      amount_usd = row['amount'] * row['price']
      # round amount to cents and cummulate in running balance
      rounded_amount_usd = amount_usd #round(amount_usd, 2)
      running_balance += amount_usd - rounded_amount_usd

      # write mining entry if amount larger 0.00
      if rounded_amount_usd > 0:
        entry_mining['Umsatz (ohne Soll/Haben-Kz)'] = rounded_amount_usd
        entry_mining['Soll/Haben-Kennzeichen'] = 'S'
        entry_mining['Konto'] = 1101
        entry_mining['Gegenkonto (ohne BU-Schlüssel)'] = 8101
        entry_mining['BU-Schlüssel'] = 0
        entry_mining['Belegdatum'] = datum
        entry_mining['Skonto'] = 0
        entry_mining['Buchungstext'] = f'Mining on Block {height}'

        datev_df = datev_df.append(entry_mining, ignore_index=True)

    # transfer to binance
    elif row['type'] == 'payment':
      entry_transfer = entry.copy()
      entry_fee = entry.copy()

      # transfer
      entry_transfer['Umsatz (ohne Soll/Haben-Kz)'] = round(row['amount'] * row['price'], 2)
      entry_transfer['Soll/Haben-Kennzeichen'] = 'H'
      entry_transfer['Konto'] = 'Binance Konto'
      entry_transfer['Gegenkonto (ohne BU-Schlüssel)'] = 1101
      entry_transfer['BU-Schlüssel'] = 0
      entry_transfer['Belegdatum'] = datum
      entry_transfer['Skonto'] = 0
      entry_transfer['Buchungstext'] = f'Transfer on Block {height}'

      # transfer fee
      entry_fee['Umsatz (ohne Soll/Haben-Kz)'] = row['fee_usd']
      entry_fee['Soll/Haben-Kennzeichen'] = 'H'
      entry_fee['Konto'] = 'Transaktionskosten Konto'
      entry_fee['Gegenkonto (ohne BU-Schlüssel)'] = 1101
      entry_fee['BU-Schlüssel'] = 0
      entry_fee['Belegdatum'] = datum
      entry_fee['Skonto'] = 0
      entry_fee['Buchungstext'] = f'Transfer on Block {height}'

      datev_df = datev_df.append(entry_transfer, ignore_index=True)
      datev_df = datev_df.append(entry_fee, ignore_index=True)

    else:
      type_e = row['type']
      print(f'Error in Export {type_e}')

    

  entry_bal = entry.copy()

  entry_bal['Umsatz (ohne Soll/Haben-Kz)'] = round(running_balance, 2)
  entry_bal['Soll/Haben-Kennzeichen'] = 'S'
  entry_bal['Konto'] = 1101
  entry_bal['Gegenkonto (ohne BU-Schlüssel)'] = 8101
  entry_bal['BU-Schlüssel'] = 0
  entry_bal['Belegdatum'] = datum
  entry_bal['Skonto'] = 0
  entry_bal['Buchungstext'] = f'Correction for smaller events'

  datev_df = datev_df.append(entry_bal, ignore_index=True)


  file_path = export_path + '.xlsx'
  datev_df.to_excel(file_path, index=False, startrow=1)

  # modify header
  wb = openpyxl.load_workbook(file_path)
  ws = wb.active

  ws['A1'] = 'EXTF'
  ws['B1'] = 700
  ws['C1'] = 21
  ws['D1'] = 'Buchungsstapel'
  ws['E1'] = 11
  ws['F1'] = datetime.now().strftime('%Y%m%d%H%M%S000')
  # G1
  ws['H1'] = 'RE'
  ws['I1'] = 'Niklas Cao'
  ws['K1'] = 0
  ws['L1'] = 10007
  ws['M1'] = date(date.today().year, 1, 1).strftime('%Y%m%d')
  ws['N1'] = '?'
  ws['O1'] = date(year, month, 1).strftime('%Y%m%d')
  ws['P1'] = date(year, month, calendar.monthrange(year, month)[-1]).strftime('%Y%m%d')
  ws['Q1'] = 'Helium'
  ws['S1'] = 1
  ws['T1'] = 0
  ws['U1'] = 0
  ws['V1'] = 'USD'

  wb.save(file_path)


  return None