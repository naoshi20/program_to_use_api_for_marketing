!pip install --upgrade google-api-python-client
!pip install oauth2client
import pandas as pd
import datetime
from calendar import monthrange
from google.colab import drive
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
# 認証処理
drive.mount('/content/drive')
KEY_FILE_LOCATION = '/content/drive/My Drive/*****'
url = '<あなたが権限を持つWebサイトのurl>'
# 取得データの日付
today = datetime.datetime.today()
# 今月の1日
thismonth = datetime.datetime(today.year, today.month, 1)
# 前月末日
lastmonth = thismonth + datetime.timedelta(days=-1)
# 先月の1日-28日
start_date = datetime.datetime.strftime(lastmonth.replace(day=1), '%Y-%m-%d')
end_date = datetime.datetime.strftime(lastmonth.replace(day=28), '%Y-%m-%d')
# 先月の末日
end_date = datetime.datetime.strftime(lastmonth.replace(day=monthrange(lastmonth.year, lastmonth.month)[1]), '%Y-%m-%d')

# Google Analytics API
from apiclient.discovery import build

def make_GA_sheet_title(d_list):
  title_txt = ''
  tool = 'GA'
  d_list_revised= list(map(lambda x : x.split('ga:')[1], d_list))
  for d in d_list_revised:
    title_txt = title_txt + '_' + d
  sheet_title = 'raw_' + tool + title_txt
  return sheet_title


def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service

def get_first_profile_id(service):
    # Use the Analytics service object to get the first profile id.

    # Get a list of all Google Analytics accounts for this user
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')

        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
                accountId=account).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')

            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(
                    accountId=account,
                    webPropertyId=property).execute()

            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')

    return None


def get_results(service, profile_id, d_list, m_list, row_limit, sort_key):
    # Use the Analytics Service Object to query the Core Reporting API
    # for the number of sessions within the past seven days.
    metrics = m_list[0]
    for m in m_list[1:]:
      metrics = metrics + ',' + m
    dimensions = d_list[0]
    for d in d_list[1:]:
      dimensions = dimensions + ',' + d
    return service.data().ga().get(
            ids='ga:' + profile_id,
            start_date=start_date,#'7daysAgo'
            end_date=end_date,#'today'
            metrics=metrics,#'ga:sessions,ga:users'
            dimensions=dimensions,
            max_results=row_limit,# 最大件数
            sort=sort_key).execute()


def print_results(results):
    # Print data nicely for the user.
    if results:
        print('View (Profile):', results.get('profileInfo').get('profileName'))
        print('Total Sessions:', results.get('rows'))#[0][0]

    else:
        print('No results found')


def GA_main(d_list, m_list, row_limit, sort_key='ga:users'):
    # Define the auth scopes to request.
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    key_file_location = '/content/drive/' + path_json

    # Authenticate and construct service.
    service = get_service(
            api_name='analytics',
            api_version='v3',
            scopes=[scope],
            key_file_location=key_file_location)

    profile_id = get_first_profile_id(service)
    results = get_results(service, profile_id, d_list, m_list, row_limit, sort_key)#print_results(results)
    rows = results.get('rows')
    columns = list(map(lambda x : x.split('ga:')[1], d_list + m_list))
    df = pd.DataFrame(rows, columns=columns)
    return df

# Google Search Console API
def make_SC_sheet_title(d_list):
  title_txt = ''
  tool = 'SC'
  for d in d_list:
    title_txt = title_txt + '_' + d
  sheet_title = 'raw_' + tool + title_txt
  return sheet_title

def SC_main(d_list,row_limit):
  SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, scopes=SCOPES)
  webmasters = build('webmasters', 'v3', credentials=credentials)

  body = {
    'startDate': start_date,
    'endDate': end_date,
    'dimensions': d_list,
    'rowLimit': row_limit}

  response = webmasters.searchanalytics().query(siteUrl=url, body=body).execute()
  df = pd.io.json.json_normalize(response['rows'])

  for i, d in enumerate(d_list):
      df[d] = df['keys'].apply(lambda x: x[i])

  df.drop(columns='keys', inplace=True)
  return df


# Google Spreadsheet API
import gspread


def get_client_spread(scopes, key_file_location):
  credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
  client = gspread.authorize(credentials)
  return client

def get_worksheet(client, sheet_title, df):
  #ブック
  workbook_name = 'raw'
  workbook = client.open(workbook_name)
  title = "{}_{}_{}".format(sheet_title, start_date, end_date)
  #シート
  worksheets = workbook.worksheets() # シート一覧の取得。
  for sheet in worksheets: #同名ワークシートは削除。
    if title == sheet.title:
      workbook.del_worksheet(sheet)
    else:
      pass#ワークシート作成
  count_row = df.shape[0]
  count_col = df.shape[1]
  worksheet_write = workbook.add_worksheet(title=title, rows=str(count_row+1), cols=str(count_col)) #コラムを一行目に入れる。インデックスは含めない。
  return worksheet_write

def export_to_sheet(worksheet_write, df):
  #出力データ
  export_values = list(df.values.tolist())
  col_list = df.columns.values.tolist()
  #出力
  count_row = df.shape[0]
  count_col = df.shape[1]
  for i in list(range(count_col)):
    worksheet_write.update_cell(1, i+1, col_list[i])
  counter = 1
  for i in list(range(count_row)):
    for j in list(range(count_col)):
      if counter % 90 == 0:
        time.sleep(100)
      worksheet_write.update_cell(i+2, j+1, export_values[i][j])
      counter += 1


def sheet_main(df,sheet_title):
    # Define the auth scopes to request.
    scopes = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']#'https://googleapis.com/auth/drive' 権限の選択。
    key_file_location = '/content/drive/' + path_json
    #sheet_title = make_GA_sheet_title()
    # Authenticate and construct service.
    client = get_client_spread(
            scopes=scopes,
            key_file_location=key_file_location)
    worksheet_write = get_worksheet(
        client=client,
        sheet_title=sheet_title,
        df=df)
    export_to_sheet(
        worksheet_write=worksheet_write,
        df=df)
    return Nonedef sheet_main(df,sheet_title):
    # Define the auth scopes to request.
    scopes = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']#'https://googleapis.com/auth/drive' 権限の選択。
    key_file_location = '/content/drive/' + path_json
    #sheet_title = make_GA_sheet_title()
    # Authenticate and construct service.
    client = get_client_spread(
            scopes=scopes,
            key_file_location=key_file_location)
    worksheet_write = get_worksheet(
        client=client,
        sheet_title=sheet_title,
        df=df)
    export_to_sheet(
        worksheet_write=worksheet_write,
        df=df)
    return None


d_list = ['ga:pagePath']
m_list = ['ga:users'] #'ga:pageviews','ga:uniquePageviews','ga:avgTimeOnPage','ga:entrances','ga:exitRate','ga:bounceRate','ga:sessions',
row_limit = 5000
df = GA_main(d_list, m_list, row_limit, sort_key='-ga:users')
sheet_title = make_GA_sheet_title(d_list)
sheet_main(df, sheet_title)

d_list = ['query','page']
row_limit = 5000
df = SC_main(d_list,row_limit)
sheet_title = make_SC_sheet_title(d_list)
sheet_main(df, sheet_title)
