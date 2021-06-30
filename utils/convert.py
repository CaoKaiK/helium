
from datetime import date, datetime
import calendar
import openpyxl
import pandas as pd

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

def datev_row(umsatz, s_h, konto, gegenkonto, datum, buchungstext):
  # empty entry dict
  entry = dict.fromkeys(datev_col, '')

  # write entry
  entry['Umsatz (ohne Soll/Haben-Kz)'] = umsatz
  entry['Soll/Haben-Kennzeichen'] = s_h
  entry['Konto'] = konto
  entry['Gegenkonto (ohne BU-Schlüssel)'] = gegenkonto
  entry['BU-Schlüssel'] = 0
  entry['Belegdatum'] = datum
  entry['Skonto'] = 0
  entry['Buchungstext'] = buchungstext

  return entry

def convert_df_to_datev(wallet_df, export_path):
  # dataframe
  datev_df = pd.DataFrame(columns=datev_col)
  
  # monthly correction for amounts below 0.01$
  running_balance = 0

  # iterate events in helium wallet
  for index, row in wallet_df.iterrows():
    height = row['height']
    year = row['year']
    month = row['month']
    day = row['day']
    datum = int(str(day) + str(month).zfill(2))
    
    ### mining ###
    if row['type'] == 'mining':
      #
      amount_usd = row['amount'] * row['price']
      # round amount to cents and cummulate difference in running balance
      rounded_amount_usd = round(amount_usd, 2)
      running_balance += amount_usd - rounded_amount_usd

      # write mining entry only if rounded amount greater 0
      if rounded_amount_usd > 0:
        # prepare and append entry
        entry = datev_row(rounded_amount_usd, 'S', 1500, 8100, datum, f'Mining on block {height}')
        datev_df = datev_df.append(entry, ignore_index=True)

    ### transfer ###
    elif row['type'] == 'payment':
      
      # outgoing transfer
      if row['amount'] > 0:
        rounded_amount_usd = round(row['amount'] * row['price'], 2)
        # prepare and append entry
        entry = datev_row(rounded_amount_usd, 'S', 1500, 'zu Extern', datum, f'Transfer from Helium Wallet on Block {height}')
        datev_df = datev_df.append(entry, ignore_index=True)
        # prepare and add transaction fee
        entry = datev_row(row['fee_usd'], 'S', 1500, 'Gebühren', datum, f'Transcation Fee on Block {height}')
        datev_df = datev_df.append(entry, ignore_index=True)


      # incoming transfer
      else:
        # positive amount required for balancing
        rounded_amount_usd = abs(round(row['amount'] * row['price'], 2))
        # prepare and append entry
        entry = datev_row(rounded_amount_usd, 'S', 1101, 'von Extern', datum, f'Transfer to Helium Wallet on Block {height}')
        datev_df = datev_df.append(entry, ignore_index=True)


    elif row['type'] == 'transfer_hotspot':
      # prepare and add transaction fee
      entry = datev_row(row['fee_usd'], 'S', 1500, 'Gebühren', datum, f'Tranfer Hotspot on Block {height}')
      datev_df = datev_df.append(entry, ignore_index=True)

    else:
      type_e = row['type']
      print(f'Error in Export {type_e}')

    
  # running balance at end of month
  entry = datev_row(round(running_balance, 2), 'S', 1500, 8100, datum, f'Korrekturbuchung für Summe der Erträge kleiner 0.01$')
  datev_df = datev_df.append(entry, ignore_index=True)

  file_path = export_path + '.xlsx'
  datev_df.to_excel(file_path, index=False, startrow=1)

  # # modify header
  # wb = openpyxl.load_workbook(file_path)
  # ws = wb.active

  # ws['A1'] = 'EXTF'
  # ws['B1'] = 700
  # ws['C1'] = 21
  # ws['D1'] = 'Buchungsstapel'
  # ws['E1'] = 11
  # ws['F1'] = datetime.now().strftime('%Y%m%d%H%M%S000')
  # # G1
  # ws['H1'] = 'RE'
  # ws['I1'] = 'Niklas Cao'
  # ws['K1'] = 0
  # ws['L1'] = 10007
  # ws['M1'] = date(date.today().year, 1, 1).strftime('%Y%m%d')
  # ws['N1'] = '?'
  # ws['O1'] = date(year, month, 1).strftime('%Y%m%d')
  # ws['P1'] = date(year, month, calendar.monthrange(year, month)[-1]).strftime('%Y%m%d')
  # ws['Q1'] = 'Helium'
  # ws['S1'] = 1
  # ws['T1'] = 0
  # ws['U1'] = 0
  # ws['V1'] = 'USD'

  # wb.save(file_path)

  return None