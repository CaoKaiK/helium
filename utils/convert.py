
from datetime import date, datetime
import calendar
import openpyxl
import pandas as pd

# columns for datev export
datev_col = [
  'Umsatz (ohne Soll/Haben-Kz)',
  'Soll/Haben-Kennzeichen',
  'WKZ Umsatz',
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
  
  # monthly correction for amounts below 0.01€
  running_balance = 0

  # iterate events in helium wallet
  for index, row in wallet_df.iterrows():
    height = row['height']
    year = row['year']
    month = row['month']
    day = row['day']
    datum = int(str(day) + str(month).zfill(2))
    
    ### MINING ###
    if row['type'] == 'mining':
      amount_hnt = row['amount']
      price = row['price_eur'] # price
      amount_currency = amount_hnt * price / 1e8

      # round amount to cents and cummulate difference in running balance
      rounded_amount_currency = round(amount_currency, 2)
      running_balance += amount_currency - rounded_amount_currency

      # write mining entry only if rounded amount greater 0
      if rounded_amount_currency > 0:
        # prepare and append entry
        entry = datev_row(rounded_amount_currency, 'S', 1500, 8100, datum, f'Mining|{amount_hnt/1e8:.3f} HNT|Block {height}|Kurs {price:.3f} €/HNT')
        datev_df = datev_df.append(entry, ignore_index=True)

    ### TRANSFER ###
    elif row['type'] == 'transaction':

      price_at_transaction = row['price_eur'] # 'price'
      transaction_fee = row['fee_eur'] # 'fee_usd'
      transaction_fee_hnt = row['fee_hnt']
      
      ### --> OUTGOING --> ###
      if row['amount'] < 0:
        ### fee ###
        for cost_neutral_part, realized_part in zip(row['cost_neutral'], row['realized']):
          # heights should match
          neutral_height = cost_neutral_part['height']
          earning_height = realized_part['height']
          purchase_price = realized_part['price_eur'] # 'price'
          # amount hnt
          amount_hnt = cost_neutral_part['amount_hnt']

          # realized losses
          if realized_part['amount_eur'] < 0:
            
            entry = datev_row(cost_neutral_part['amount_eur'], 'S', 2325, 1500, datum, f'Transaktion|Neutral|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Ein{purchase_price:.3f} €/HNT')
            datev_df = datev_df.append(entry, ignore_index=True)

            entry = datev_row(cost_neutral_part['amount_eur']+realized_part['amount_eur'], 'S', 1100, 2326, datum, f'Transaktion|Verlust|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Aus {price_at_transaction:.3f} €/HNT')
            datev_df = datev_df.append(entry, ignore_index=True)
          else:
            entry = datev_row(cost_neutral_part['amount_eur'], 'S', 2726, 1500, datum, f'Transaktion|Neutral|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Ein {purchase_price:.3f} €/HNT')
            datev_df = datev_df.append(entry, ignore_index=True)

            entry = datev_row(cost_neutral_part['amount_eur']+realized_part['amount_eur'], 'S', 1100, 2725, datum, f'Transaktion|Gewinn|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Aus {price_at_transaction:.3f} €/HNT')
            datev_df = datev_df.append(entry, ignore_index=True)
        
        # transaction fee
        entry = datev_row(transaction_fee, 'S', 4909, 1100, datum, f'Transaktion|Transaktiongebuehr in €|Total: {transaction_fee_hnt/1e8:.3f} HNT')
        datev_df = datev_df.append(entry, ignore_index=True)

        # transfer HNT from helium to Binance Wallet
        amount_hnt = row['amount']
        entry = datev_row(row['pseudo_neutral_amount_eur'], 'S', 1363, 1500, datum, f'Transaktion|Uebertrag {-amount_hnt/1e8:.3f} HNT von Helium auf Binance Wallet')
        datev_df = datev_df.append(entry, ignore_index=True)

        entry = datev_row(row['pseudo_neutral_amount_eur'], 'S', 1501, 1363, datum, f'Transaktion|Uebertrag {-amount_hnt/1e8:.3f} HNT von Helium auf Binance Wallet')
        datev_df = datev_df.append(entry, ignore_index=True)
          

      ### <-- INCOMING <-- ###
      else:
        amount_hnt = row['amount']
        price = row['price_eur'] # 'price'
        rounded_amount = round(amount_hnt / 1e8 * price, 2)
        # prepare and append entry
        entry = datev_row(rounded_amount, 'S', 1500, 703, datum, f'Transfer auf Helium Wallet|{amount_hnt/1e8:.3f} HNT|Block {height}|Kurs {price:.3f} €/HNT')
        datev_df = datev_df.append(entry, ignore_index=True)

    ### SERVICE ###
    elif row['type'] in ['transfer_hotspot', 'assert_location']:

      price_at_transaction = row['price_eur'] # 'price'
      transaction_fee = row['fee_eur'] # 'fee_usd
      transaction_fee_hnt = row['fee_hnt']

      if row['type'] == 'transfer_hotspot':
        description = 'Typ 1'
      elif row['type'] == 'assert_location':
        description = 'Typ 2'
      else:
        description = 'Undefined'

      for cost_neutral_part, realized_part in zip(row['cost_neutral'], row['realized']):
        # heights should match
        neutral_height = cost_neutral_part['height']
        earning_height = realized_part['height']
        purchase_price = realized_part['price_eur'] # 'price'
        # amount hnt
        amount_hnt = cost_neutral_part['amount_hnt']
        # realized losses
        if realized_part['amount_eur'] < 0:
          entry = datev_row(cost_neutral_part['amount_eur'], 'S', 2325, 1500, datum, f'{description}|Neutral|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Ein {purchase_price:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
          entry = datev_row(cost_neutral_part['amount_eur']+realized_part['amount_eur'], 'S', 2326, 1100, datum, f'{description}|Verlust|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Aus {price_at_transaction:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
        else:
          entry = datev_row(cost_neutral_part['amount_eur'], 'S', 2726, 1500, datum, f'{description}|Neutral|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Ein {purchase_price:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
          entry = datev_row(cost_neutral_part['amount_eur']+realized_part['amount_eur'], 'S', 1100, 2725, datum, f'{description}|Gewinn|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Aus {price_at_transaction:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
        
      # transaction fee in USD
      entry = datev_row(transaction_fee, 'S', 4900, 1100, datum, f'{description}|Servicegebuehr in EUR|Total:{transaction_fee_hnt/1e8:.3f} HNT')
      datev_df = datev_df.append(entry, ignore_index=True)

    ### EXCHANGE ###
    elif row['type'] == 'exchange':

      price_at_transaction = row['price_eur'] # 'price_eur'
      total_amount = row['amount']
      usd_eur = row['usd_eur']

      balance_1502 = 0
      for cost_neutral_part, realized_part in zip(row['cost_neutral'], row['realized']):
        # heights should match
        neutral_height = cost_neutral_part['height']
        earning_height = realized_part['height']
        purchase_price = realized_part['price_eur'] # 'price'
        # amount hnt
        amount_hnt = cost_neutral_part['amount_hnt']
        # USD balance
        balance_1502 += realized_part['amount_eur']

        # realized losses
        if realized_part['amount_eur'] < 0:
          
          entry = datev_row(cost_neutral_part['amount_eur'], 'S', 2325, 1501, datum, f'Umtausch|Neutral|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Ein {purchase_price:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
          entry = datev_row(cost_neutral_part['amount_eur']+realized_part['amount_eur'], 'S', 2326, 1502, datum, f'Umtausch|Verlust|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Aus {price_at_transaction:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
          
        else:
          entry = datev_row(cost_neutral_part['amount_eur'], 'S', 2726, 1501, datum, f'Umtausch|Neutral|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Ein {purchase_price:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
          entry = datev_row(cost_neutral_part['amount_eur']+realized_part['amount_eur'], 'S', 1502, 2725, datum, f'Umtausch|Gewinn|{amount_hnt/1e8:.3f} HNT|Block {neutral_height}|Kurs Aus {price_at_transaction:.3f} €/HNT')
          datev_df = datev_df.append(entry, ignore_index=True)
      # TODO
      # Umtausch USD in EUR und Überweisung an Konto inkl. Überweisungskosten
      entry = datev_row(balance_1502, 'S', 2726, 1502, datum, f'Umtausch|Neutral|{balance_1502:.3f} USDT')
      datev_df = datev_df.append(entry, ignore_index=True)

      entry = datev_row(0, 'S', 1101, 2725, datum, f'Umtausch|Gewinn|{balance_1502:.3f} USDT|Kurs {usd_eur} USDT/€')
      datev_df = datev_df.append(entry, ignore_index=True)

      transaction_fee_eur = row['transaction_fee_eur']
      entry = datev_row(transaction_fee_eur, 'S', 4970, 1101, datum, f'Transaktion|Transaktiongebuehr in EUR|Total: {transaction_fee_eur} €')
      datev_df = datev_df.append(entry, ignore_index=True)

      # euro_transferred = round(total_amount * price_at_transaction * usd_eur / 1e8 - 0.8, 2)
      transaction_amount = row['transaction_amount'] - transaction_fee_eur

      entry = datev_row(transaction_amount, 'S', 1364, 1101, datum, f'Transaktion|Uebertrag {transaction_amount} € von Binance nach Penta')
      datev_df = datev_df.append(entry, ignore_index=True)

      entry = datev_row(transaction_amount, 'S', 1200, 1364, datum, f'Transaktion|Uebertrag {transaction_amount} € von Binance nach Penta')
      datev_df = datev_df.append(entry, ignore_index=True)


    else:
      type_e = row['type']
      print(f'Error in Export {type_e}')

    
  # running balance at end of month
  if round(running_balance, 2) > 0:
    entry = datev_row(round(running_balance, 2), 'S', 1500, 8100, datum, f'Korrekturbuchung für Summe der Erträge kleiner 0.01€')
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