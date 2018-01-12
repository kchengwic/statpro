import requests
import requests.auth
import json
import time
from datetime import date, timedelta, datetime
import holidays
import re
import pandas as pd

class Portfolio(object):
    name = None
    earlist_date = None
    most_recent_date = None
    default_analysis_link = None

    def __init__(self, p_name, portfolio_analysis_link):
        self.name = p_name
        self.default_analysis_link = portfolio_analysis_link


url = 'https://revapiaccess.statpro.com/OAuth2/Token'
webapi_entry_point_uri = "https://revapi.statpro.com/v1"

"""
Retrieving access_token and refresh_token from Token API.
"""
def retrieve_tokens():
    access_token_req = {"grant_type": "password",
                      "username": "datafeed.5471da97@noreply-statpro.com",
                      "password": "zu(gZ{^GZ3DG%D{t9[oI}2$iU|H)$S%OhDP=]|I$Y;]%C&pOE_55SwQq#gb;Zx/!",
                      "scope": 'RevolutionWebApi'}
    client_id = "wicfunds"
    client_secret = "d13EJqirtGne90plBj8"
    client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    r = requests.post(url, auth=client_auth, data=access_token_req)
    data = json.loads(r.text)
    return data['access_token']

def retrieve_csv_from_webapi(token, uri):
    headers = {"Authorization": "Bearer " + token}
    r = requests.get(uri, headers=headers)
    return r

def retrieve_resource_from_webapi(token, uri, data=None, headers=None, request_method='get'):
    if not headers:
        headers = {"Authorization": "Bearer " + token,
                   "Accept": "application/json"}
    if request_method == 'get':
        r = requests.get(uri, data=data, headers=headers)
    else:
        r = requests.post(uri, data=data, headers=headers)
    if not r.raise_for_status():
        data = json.loads(r.text)
    else:
        raise Exception(r.raise_for_status())
    return data

def print_json(data):
    print json.dumps(data, indent=4, sort_keys=True)

def retrieve_portfolios_query_uri(data):
    portfolios_query_uri = data['service']['portfolios']['links']['portfoliosQuery']['href']
    replaced_list = ['{filter}', '{orderby}', '{skip}', '{top}']
    for word in replaced_list:
        portfolios_query_uri = portfolios_query_uri.replace(word, '')
    return portfolios_query_uri

def get_portfolio_uri_dict(data):
    portfolios_dict = dict()
    portfolios = data['portfolios']['items']
    for p in portfolios:
        portfolio = Portfolio(p['name'], p['links']['defaultAnalysis']['href'])
        # portfolios_dict[p['name']] = p['links']['defaultAnalysis']['href']
        portfolios_dict[portfolio.name] = portfolio
    print '%s portfolios are returned: %s' % (len(portfolios_dict.keys()), portfolios_dict.keys())
    return portfolios_dict

def retrieve_uri(data, type, keyword_dict=None):
    uri = data['portfolioAnalysis']['analysis']['results']['links'][type]['href']
    if keyword_dict:
        for key, value in keyword_dict.iteritems():
            uri = uri.replace(key, value)
    print uri
    return uri

def write_to_file(filename, text):
    f = open(filename, 'wb')
    f.write(text)
    f.close()

def generate_xml(ids, measures_list='Rp,Ctp,Wp', start_date='2017-10-13', end_date='2017-11-13'):
    body_xml = "<multipleOcpRequest><measures>" + measures_list + "</measures><timePeriods><timePeriod><name>time</name>"\
               + "<start>" + start_date + "</start><end>" + end_date + "</end></timePeriod> </timePeriods><segments>"
    for id in ids:
        body_xml += "<segment><id>" + str(id) + "</id><periods>time</periods></segment>"
    body_xml += "</segments></multipleOcpRequest>"
    return body_xml

"""generate date pairs, e.g. start date = 2017-10-13 and end date = 2017-10-16
this removes all the weekend and federal holidays"""
def generate_dates(start_date, end_date):
    dates_dict = dict()
    dates_list = []
    # day_dict = {0: 'Mon', 1: 'Tues', 2: 'Wed', 3: 'Thur', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    # us_holidays = holidays.UnitedStates()
    s_date = datetime.strptime(start_date, '%Y-%m-%d')
    e_date = datetime.strptime(end_date, '%Y-%m-%d')
    delta = e_date - s_date
    for i in range(delta.days + 1):
        next_date = s_date + timedelta(days=i)
        if next_date.weekday() >= 5:# or us_holidays.get(next_date):
            continue
        next_date_str = next_date.strftime('%Y-%m-%d')
        dates_list.append(next_date_str)
    for i in range(0, len(dates_list) - 1):
        dates_dict[i] = [dates_list[i], dates_list[i+1]]
    return dates_dict

def generate_custom_time_period_xml(start_date, end_date):
    body_xml = "<customTimePeriodAnalysisRequest xmlns=\"http://statpro.com/2012/Revolution\">"\
               + "<timePeriods>"
    dates = generate_dates(start_date, end_date)
    for num, date in dates.items():
        body_xml += "<timePeriod><start>" + date[0] + "</start>"
        body_xml += "<end>" + date[1] + "</end></timePeriod>"
    body_xml += "</timePeriods><applyToReportingComponents>true</applyToReportingComponents></customTimePeriodAnalysisRequest>"
    return body_xml

def get_segment_tree_data(data, print_data=False):
    time_code = data['portfolioAnalysis']['analysis']['results']['timePeriods'][0]['code']
    segment_tree_keyword_dict = {'{timePeriodsList}': time_code,
                                 '{measuresList}': 'Rp,Ctp,Wp,Classification3Name',
                                 '{dataToInclude}': 'securities',
                                 '{measuresFor}': 'securities',
                                 '{filter}': '',
                                 '{orderby}': '',
                                 '{skip}': '',
                                 '{top}': ''}
    segment_tree_data_uri = retrieve_uri(data, 'segmentsTreeRootNodeQuery', segment_tree_keyword_dict)
    segment_tree_data = retrieve_resource_from_webapi(token, segment_tree_data_uri)
    if print_data:
        print_json(segment_tree_data)
    return segment_tree_data

def get_portfolio_date_dict(portfolios_dict):
    for p_name, p in portfolios_dict.items():
        fund_data = retrieve_resource_from_webapi(token, p.default_analysis_link)
        # print_json(fund_data)
        try:
            time_periods = fund_data['portfolioAnalysis']['analysis']['results']['timePeriods']
            for period in time_periods:
                if 'Earliest' in period['code']:
                    p.earlist_date = period['startDate']
                    p.most_recent_date = period['endDate']
                    portfolios_dict[p_name] = p
                    break
        except Exception as e:
            print p_name + ' does not have time periods found in portfolioAnalysis response.'
    return portfolios_dict

def get_portfolios(token):
    data = retrieve_resource_from_webapi(token, webapi_entry_point_uri)
    portfolios_query_uri = retrieve_portfolios_query_uri(data)
    data = retrieve_resource_from_webapi(token, portfolios_query_uri)
    # print_json(data)
    portfolios_dict = get_portfolio_uri_dict(data)
    portfolios_dict = get_portfolio_date_dict(portfolios_dict)
    return portfolios_dict

def get_custom_time_periods_portfolio(token, portfolio_analysis_link, start_date, end_date):
    fund_data = retrieve_resource_from_webapi(token, portfolio_analysis_link)
    # print_json(fund_data)

    """get portfolioAnalysisControllerQuery uri"""
    uri = fund_data['portfolioAnalysis']['links']['portfolioAnalysisControllerQuery']['href']
    time_xml = generate_custom_time_period_xml(start_date, end_date)
    port_analysis_controller_data = retrieve_resource_from_webapi(token, uri, data=time_xml, request_method='post')

    """get customTimePeriodPortfolioAnalysis"""
    uri = port_analysis_controller_data['links']['customTimePeriodPortfolioAnalysis']['href']
    while True:
        custom_time_period_data = retrieve_resource_from_webapi(token, uri)
        status = custom_time_period_data['portfolioAnalysis']['analysis']['status']
        if status == 'InProgress':
            print 'StatPro is still retrieving the data... wait for 10 seconds'
            time.sleep(10)
        else:
            break
    return status, custom_time_period_data

def get_custom_time_period_securities_data(custom_time_period_data, portfolio, write=True):
    all_text = ''
    time_periods_list = custom_time_period_data['portfolioAnalysis']['analysis']['results']['timePeriods']
    day_count = 0
    for time in time_periods_list:
        time_code = time['code']
        segment_tree_keyword_dict = {'{timePeriodsList}': time_code,
                                     '{measuresList}': 'Rp,Ctp,Wp,Classification3Name',
                                     '{dataToInclude}': 'Securities'}
        arb_fund_whole_segment_data_uri = retrieve_uri(custom_time_period_data, 'wholeSegmentsTreeQuery',
                                                       segment_tree_keyword_dict)
        arb_fund_whole_segment_data = retrieve_csv_from_webapi(token, arb_fund_whole_segment_data_uri)
        segment_date_txt = arb_fund_whole_segment_data.text
        all_text += segment_date_txt
        day_count += 1
    return all_text

def parse_date_for_df(res_df):
    s_index = 0
    dates = res_df.columns[5].split(':')
    s_date = dates[0].split('SpecificDate-')[1]
    e_date = dates[1].split('SpecificDate-')[1]
    header_index = list(res_df[res_df['isSecurity'] == 'isSecurity'].index)
    header_index.append(res_df.index[-1])
    for index in header_index:
        for i in range(s_index, index+1):
            res_df._set_value(i, 'From', s_date)
            res_df._set_value(i, 'To', e_date)
        s_index = index
        if index == header_index[-1]:
            break
        dates = res_df.iloc[s_index, 5].split(':')
        s_date = dates[0].split('SpecificDate-')[1]
        e_date = dates[1].split('SpecificDate-')[1]
    # replacing the old headers with new headers. it removed the date from the column names
    new_col = []
    for c in res_df.columns:
        new_c = c
        if 'SpecificDate' in c:
            new_c = c.split('.SpecificDate-')[0]
        new_col.append(new_c)
    res_df.columns = new_col
    res_df = res_df[res_df['isSecurity'] != 'isSecurity']
    res_df = res_df.drop(['isSecurity', 'id', 'parentId'], axis=1)
    return res_df

if __name__ == '__main__':
    token = retrieve_tokens()
    portfolios_dict = get_portfolios(token)

    """
    Litman Gregory Master Alternative Strategy - Water
    Arbitrage Fund by Region
    The Arbitrage Tactical Equity Fund
    Transamerica Arbitrage Strategy
    Water Island Credit Portfolio
    The Arbitrage Credit Opportunities Fund
    The Arbitrage Event-Driven Fund
    TACO - Top/Bottom
    CAM - by Asset Class
    The Arbitrage Fund <--------------------------- WIP
    Long/Short by Strategy
    AED Top-Bottom
    Columbia AP Multi Manager Alternative Strategy - W <--------------------------- done
    """
    p_name = 'The Arbitrage Fund'  # TODO replace this line with the names above
    portfolio = portfolios_dict[p_name]
    print 'Earliest Date: ' + portfolio.earlist_date + ', Latest Date: ' + portfolio.most_recent_date
    """alter the date below for each download. StatPro only allows 100-days per request and 10 such requests per hour"""
    year = '2015'
    # start_date = year + '-01-01'
    # end_date = year + '-03-31'
    # start_date = year + '-04-01'
    # end_date = year + '-06-30'
    # start_date = year + '-07-01'
    # end_date = year + '-09-30'
    start_date = year + '-10-01'
    end_date = year + '-12-31'
    # start_date = '2012-12-31'
    # end_date = '2013-03-31'
    status, custom_time_period_data = get_custom_time_periods_portfolio(token, portfolio.default_analysis_link,
                                                                     start_date, end_date)
    # print_json(custom_time_period_data)
    if 'Finished' in status:
        res_text = get_custom_time_period_securities_data(custom_time_period_data, portfolio)
        whole_seg_filename = portfolio.name + start_date + '_whole_segment_tree.csv'
        write_to_file(whole_seg_filename, res_text)
        res_df = pd.read_csv(whole_seg_filename)
        res_df = parse_date_for_df(res_df)
        res_df.to_csv(whole_seg_filename, index=False)
    else:
        print 'Error: ' + status





    """------------------------------------------development--------------------------------------------------------"""
    """
    retrieve segment tree node from the portfolio
    """
    # segment_tree_keyword_dict = {'{timePeriodsList}': 'Earliest',
    #                              '{measuresList}': 'Rp',
    #                              '{dataToInclude}': 'securities',
    #                              '{measuresFor}': 'securities',
    #                              '{filter}': '',
    #                              '{orderby}': '',
    #                              '{skip}': '0',
    #                              '{top}': '4'}
    # arb_fund_segment_tree_data_uri = retrieve_uri(arb_fund_data, 'segmentsTreeRootNodeQuery', segment_tree_keyword_dict)
    # arb_fund_segment_tree_data = retrieve_resource_from_webapi(token, arb_fund_segment_tree_data_uri)
    # print_json(arb_fund_segment_tree_data)

    """
    retrieve all segments from the portfolio
    """
    # segment_tree_keyword_dict = {'{timePeriodsList}': 'Earliest',
    #                      '{measuresList}': 'Rp',
    #                      '{dataToInclude}': 'All'}
    # arb_fund_whole_segment_data_uri = retrieve_uri(arb_fund_data, 'wholeSegmentsTreeQuery', segment_tree_keyword_dict)
    # arb_fund_whole_segment_data = retrieve_csv_from_webapi(token, arb_fund_whole_segment_data_uri)
    # whole_seg_filename = 'arb_fund_whole_segment_tree.csv'
    # # write_to_file(whole_seg_filename, arb_fund_whole_segment_data.text)
    # df = pd.read_csv(whole_seg_filename, delimiter=',', header=0)
    # ids = df['id']

    """
    retrieve all time series data for ONE segment
    """
    # ts_keyword_dict = {'{measuresList}': 'Rp',
    #                  '{startDate}': '2017-10-13',
    #                  '{endDate}': '2017-11-13',
    #                  '{seriesType}': 'Raw'}
    # arb_fund_ts_data_uri = retrieve_uri(arb_fund_data, 'timeSeriesQuery', ts_keyword_dict)
    # arb_fund_ts_data = retrieve_resource_from_webapi(token, arb_fund_ts_data_uri)
    # print_json(arb_fund_ts_data)

    """
    retrieve all multiple OCP time series data
    """
    # ids = [1029, 1060]
    # body_xml = generate_xml(ids, measures_list='Rp,Ctp,Wp', start_date='2017-10-13', end_date='2017-10-16')
    # arb_fund_ocp_data_uri = retrieve_uri(arb_fund_data, 'multipleOcpTimeSeriesQuery')
    # arb_fund_ocp_data = retrieve_resource_from_webapi(token, arb_fund_ocp_data_uri, data=body_xml, request_method='post')
    # print_json(arb_fund_ocp_data)

# print(retrieve_tokens())