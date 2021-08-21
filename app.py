from flask import Flask, jsonify, request
import pandas as pd
import numpy as np
import datetime as dt
import json
from dateutil.parser import *


def make_connection():
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///Resources/hawaii.sqlite")
    with engine.connect() as conn:
        df1 = pd.read_sql("SELECT * FROM measurement", conn)
        df2 = pd.read_sql("SELECT * FROM station", conn)
    df1['date'] = pd.to_datetime(df1['date'])
    df1.rename(columns={'prcp':'prec', 'tobs':'temp'}, inplace=True)
    df1.drop(columns='id', inplace=True)
    df2.drop(columns='id', inplace=True)
    return(df1,df2)


def fmt_code(code_str):
    html_str = f'<code style="background-color:#E0E0E0; padding:2px">{code_str}</code>'
    return html_str


def valid_query(args, df1, df2):
    error = None
    qtype = None
    station = None
    from_dt = None
    to_dt = None

    # Test valid qtype
    if 'qtype' in args:
        qtype = args['qtype']
        if not (qtype == 'stats' or qtype == 'data'):
            error = "\n".join([f"<p>Error: Invalid {fmt_code('qtype')} '{qtype}'. ",
                f"Valid qtypes include {fmt_code('stats')} ",
                f"and {fmt_code('data')}.</p>",
                f"<p>See API documentation for Precipitation route.</p>"])
    else:
        error = "\n".join([f"<p>Error: You must specify the {fmt_code('qtype')} for this query. ",
            f"Valid qtypes include {fmt_code('stats')} ",
            f"and {fmt_code('data')}.</p>",
            f"<p>See API documentation for Precipitation route.</p>"])

    # Test valid station assuming valid qtype
    if qtype:
        if 'station' in args:
            station = args['station']
            if not station in df2['station'].values:
                error = "\n".join([f"<p>Error: Invalid {fmt_code('station')} '{station}'. </p>",
                    f"<p>See {fmt_code('/api/v1.0/stations')} for valid stations.</p>"])
        else:
            error = "\n".join([f"<p>Error: You must specify the {fmt_code('station')} for this query. ",
                f"<p>See API documentation for Precipitation route.</p>"])

    # Test dates assuming valid qtype and station
    if qtype and station:

        # Test valid from_date
        min_date = df1[df1['station'] == station]['date'].min().date()
        if 'from_date' in args:
            from_date = args['from_date']
            try:
                from_dt = parse(from_date).date()
                if from_dt < min_date:
                    error = "\n".join([f"<p>Error: Invalid {fmt_code('from_date')} '{from_date}'.</p><p>{fmt_code('from_date')} must occur ",
                        f"on or after the first available date {min_date.strftime('%Y-%m-%d')} for station {station}.</p>",
                        f"<p> See {fmt_code('/api/v1.0/stations')} for valid date ranges for each station.</p>"])
            except ParserError:
                error = "\n".join([f"<p>Error: Invalid {fmt_code('from_date')} format: '{from_date}'.</p>",
                                   f"<p>See API documentation for Precipitation route.</p>"])
        else:
            from_dt = min_date
            
        # Test valid to_date
        max_date = df1[df1['station'] == station]['date'].max().date()
        if 'to_date' in args:
            to_date = args['to_date']
            try:
                to_dt = parse(to_date).date()
                if to_dt > max_date:
                    error = "\n".join([f"<p>Error: Invalid {fmt_code('to_date')} '{to_date}'.</p><p>{fmt_code('to_date')} must occur ",
                        f"on or before the first available date {max_date.strftime('%Y-%m-%d')} for station {station}.</p>",
                        f"<p> See {fmt_code('/api/v1.0/stations')} for valid date ranges for each station.</p>"])
            except ParserError:
                error = "\n".join([f"<p>Error: Invalid {fmt_code('to_date')} format: '{to_date}'.</p>",
                                   f"<p>See API documentation for Precipitation route.</p>"])
            if to_dt <= from_dt:
                error = "\n".join([f"<p>Error: {fmt_code('to_date')} must occur after {fmt_code('from_date')}. ",
                                   f"({fmt_code('from_date')} = '{from_date}' and {fmt_code('to_date')} = '{to_date}')"])
        else:
            to_dt = max_date
    
    return (error, qtype, station, from_dt, to_dt)


def filter_df(df1, station, from_dt, to_dt):
    dti = pd.date_range(start=from_dt, end=to_dt, name='date')
    df_date = pd.DataFrame(index=dti)
    df_stadata = df1[df1['station'] == station].set_index('date')
    df_filt = pd.merge_ordered(df_date, df_stadata, on='date', how='left', fill_method='ffill')
    df_filt['date'] = df_filt['date'].dt.strftime('%Y-%m-%d')
    return df_filt


def get_stats(df1, column, desc, station, from_dt, to_dt):
    df_filt = filter_df(df1, station, from_dt, to_dt)
    
    quant = df_filt[column].quantile(np.arange(0,1.1,0.1))
    quant_list = []
    for key,val in quant.items():
        rndval = round(val,3)
        if key == 0:
            quant_list.append({'min': rndval})
        elif key == 1:
            quant_list.append({'max': rndval})
        else:
            quant_list.append({f"{int(round(key * 100, 0))}th": rndval})

    missing = df_filt[column].isna().sum().item()

    dict_stats = {"station": station,
                "type": desc,
                "statistics": {"from_date": from_dt.strftime('%Y-%m-%d'),
                                "to_date": to_dt.strftime('%Y-%m-%d'),
                                "days_in_range": len(df_filt),
                                "value_count": len(df_filt) - missing,
                                "missing_value_count": missing,
                                "missing_value_percent": f"{missing / len(df_filt):.2%}",
                                "mean": round(df_filt[column].mean(),3),
                                "percentiles": quant_list
                                }
                }
    return dict_stats


def get_data(df1, column, desc, station, from_dt, to_dt):
    df_filt = filter_df(df1, station, from_dt, to_dt)
    list_prec = json.loads(df_filt[['date',column]].to_json(orient='records'))
    dict_data = {"station": station,
                "type": desc,
                "data": list_prec
                }
    return dict_data



app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.route("/")
def home():
    return "\n".join(
        [
        f'<!DOCTYPE html>',
        f'<html lang="en">',
        f'<head>',
        f'  <meta charset="UTF-8">',
        f'  <title>Hawaii Climate API</title>',
        f'<style>',
        f'table {{border-collapse: collapse;}}',
        f'th {{padding: 8px; border: 1px solid white; color: white; background-color: black;}}',
        f'td {{padding: 8px; border: 1px solid black;}}',
        f'code {{background-color: #E0E0E0; padding: 2px;}}',
        f'span {{font-weight: bold; background-color: #EAEAEA; color: #EB6E4B;}}',
        f'.footnote {{font-size: 0.9em; border-right-style: none; border-left-style: none; border-bottom-style: none;}}',
        f'</style',
        f'</head>',
        f'<body>',
        f'',
        f'<div style="padding-left: 50px; padding-top: 20px; padding-bottom: 100px;">',
        f'<h1>Welcome to My Hawaii Climate API!</h1><br>',
        f'<div style="border: thin solid #BFBFBF; width: 75%;"></div>',
        f'<h2>Stations</h2><p>Obtain list of available meteorology stations and associated metadata in the API dataset. ',
        f'The following fields are provided:</p>',
        f'<ul>',
        f'   <li>Station</li>',
        f'   <li>Station name</li>',
        f'   <li>Station latitude (decimal degrees)</li>',
        f'   <li>Station longitude (decimal degrees)</li>',
        f'   <li>Station elevation (ft)</li>',
        f'   <li>Start date of available observations</li>',
        f'   <li>End date of available observations</li>',
        f'   <li>Percent of missing (undefined) precipitation observations between the start and end dates</li>',
        f'   <li>Percent of missing (undefined) temperature observations between the start and end dates</li>',
        f'</ul>',
        f'<table><thead><tr><th>Query format</th><th>Returns</th></tr></thead><tbody>',
        f'<tr><td><code>/api/v1.0/stations?mode=html</code></td>',
        f'<td>HTML table</td></tr>',
        f'<tr><td><code>/api/v1.0/stations?mode=json</code></td>',
        f'<td>JSON output of station metadata</td></tr>',
        f'</tbody></table><br><br>',
        f'<div style="border: thin solid #BFBFBF; width: 75%;"></div>',
        f'',
        f'<h2>Precipitation</h2><p>Query precipitation data from stations in the API dataset.</p>',
        f'',
        f'<h4>API Call</h4>',
        f'<table style="background-color:#EAEAEA"><tr><td style="padding: 20px">',
        f'/api/v1.0/prec?qtype=<span>&lcub;type&rcub;</span>&station=<span>&lcub;station&rcub;</span>',
        f'&from_date=<span>&lcub;start&rcub;</span>&to_date=<span>&lcub;end&rcub;</span>',
        f'</td></tr></table>',
        f'',
        f'<h4>Parameters</h4>',
        f'<table><thead><tr><th colspan="2">Parameter</th><th>Format</th><th>Description</th></tr></thead>',
        f'<tbody>',
        f'<tr><td rowspan="2"><code>qtype</code></td><td rowspan="2">Required</td><td><code>stats</code></td><td>Metadata and statistics are shown for the selected station.</td></tr>',
        f'<tr><td><code>data</code></td><td>Precipitation data for selected time range will be returned. Missing values are indicated using null.</td></tr>',
        f'<tr><td><code>station</code></td><td>Required</td><td>USC00519281</td><td>Station as shown in <code>/api/v1.0/stations</code>.</td></tr>',
        f'<tr><td><code>from_date</code></td><td>Optional</td><td>2012-05-26*</td><td>Start date for data retrieval. If omitted, the beginning of the available period of record will be used.</td></tr>',
        f'<tr><td><code>to_date</code></td><td>Optional</td><td>2016-11-03*</td><td>Start date for data retrieval. If omitted, the end of the period of available record will be used.</td></tr>',
        f'<tr><td colspan="4" class="footnote">*Dates must lie within the range shown in <code>/api/v1.0/stations</code> for the selected station.</td></tr>',
        f'</tbody></table>',
        f'',
        f'<h4>API Call Examples</h4>',
        f'<table style="background-color:#EAEAEA"><tr><td style="padding: 20px">',
        f'/api/v1.0/prec?qtype=stats&station=USC00513117&from_date=2010-06-01&to_date=2017-05-31<br><br>',
        f'/api/v1.0/prec?qtype=data&station=USC00519523&from_date=2014-01-01&to_date=2014-12-31',
        f'</td></tr></table><br><br>',
        f'<div style="border: thin solid #BFBFBF; width: 75%;"></div>',
        f'',
        f'<h2>Temperature</h2><p>Query temperature data from stations in the API dataset.</p>',
        f'',
        f'<h4>API Call</h4>',
        f'<table style="background-color:#EAEAEA"><tr><td style="padding: 20px">',
        f'/api/v1.0/temp?qtype=<span>&lcub;type&rcub;</span>&station=<span>&lcub;station&rcub;</span>',
        f'&from_date=<span>&lcub;start&rcub;</span>&to_date=<span>&lcub;end&rcub;</span>',
        f'</td></tr></table>',
        f'',
        f'<h4>Parameters</h4>',
        f'<table><thead><tr><th colspan="2">Parameter</th><th>Format</th><th>Description</th></tr></thead>',
        f'<tbody>',
        f'<tr><td rowspan="2"><code>qtype</code></td><td rowspan="2">Required</td><td><code>stats</code></td><td>Metadata and statistics are shown for the selected station.</td></tr>',
        f'<tr><td><code>data</code></td><td>Temperature data for selected time range will be returned. Missing values are indicated using null.</td></tr>',
        f'<tr><td><code>station</code></td><td>Required</td><td>USC00519281</td><td>Station as shown in <code>/api/v1.0/stations</code>.</td></tr>',
        f'<tr><td><code>from_date</code></td><td>Optional</td><td>2012-05-26*</td><td>Start date for data retrieval. If omitted, the beginning of the available period of record will be used.</td></tr>',
        f'<tr><td><code>to_date</code></td><td>Optional</td><td>2016-11-03*</td><td>Start date for data retrieval. If omitted, the end of the period of available record will be used.</td></tr>',
        f'<tr><td colspan="4" class="footnote">*Dates must lie within the range shown in <code>/api/v1.0/stations</code> for the selected station.</td></tr>',
        f'</tbody></table>',
        f'',
        f'<h4>API Call Examples</h4>',
        f'<table style="background-color:#EAEAEA"><tr><td style="padding: 20px">',
        f'/api/v1.0/temp?qtype=stats&station=USC00513117&from_date=2010-06-01&to_date=2017-05-31<br><br>',
        f'/api/v1.0/temp?qtype=data&station=USC00519523&from_date=2014-01-01&to_date=2014-12-31',
        f'</td></tr></table><br>',
        f'',
        f'</div>',
        f'</body>'
        f'</html>'
        ])

@app.route("/api/v1.0/stations")
def stations():
    df_meas, df_sta = make_connection()
    df_dates = df_meas.groupby('station').agg(start_date=('date','min'), end_date=('date','max'),
                                            prec_count=('prec','count'), temp_count=('temp','count'))
    df_dates['daterange_count'] = (df_dates['end_date'] - df_dates['start_date']).dt.days + 1
    df_dates['start_date'] = df_dates['start_date'].dt.strftime('%Y-%m-%d')
    df_dates['end_date'] = df_dates['end_date'].dt.strftime('%Y-%m-%d')
    df_dates['prec_percent_missing'] =  [f"{x:.2%}" for x in (1 - round(df_dates['prec_count'] / df_dates['daterange_count'], 4))]
    df_dates['temp_percent_missing'] = [f"{x:.2%}" for x in (1 - round(df_dates['temp_count'] / df_dates['daterange_count'], 4))]
    df_dates.drop(columns=['prec_count','temp_count','daterange_count'], inplace=True) 
    df_sta_out = df_sta.merge(df_dates, on='station', sort=True)
    if 'mode' in request.args:
        mode = request.args['mode']
        if mode == 'json':
            result = json.loads(df_sta_out.to_json(orient="records"))
            return jsonify(result)
        elif mode == 'html':
            result = df_sta_out.to_html(index=False)
            return result
        else:
            return "\n".join(
                [f"<p>Error: Invalid {fmt_code('mode')} '{mode}'. ",
                f'Valid modes include {fmt_code("json")} and {fmt_code("html")}.</p>',
                f'<p>Example: {fmt_code("/api/v1.0/stations?mode=html")}</p>'])
    else:
        return "\n".join(
            [f'<p>Error: You must specify the {fmt_code("mode")} for this query. ',
            f'Valid modes include {fmt_code("json")} and {fmt_code("html")}.</p>',
            f'<p>Example: {fmt_code("/api/v1.0/stations?mode=html")}</p>'])

@app.route("/api/v1.0/prec")
def prec():
    column = 'prec'
    desc = 'precipitation'
    df_meas, df_sta = make_connection()
    (error, qtype, station, from_dt, to_dt) = valid_query(request.args, df_meas, df_sta)
    if error:
        return error
    elif qtype == 'stats':
        result = get_stats(df_meas, column, desc, station, from_dt, to_dt)
        return jsonify(result)
    elif qtype == 'data':
        result = get_data(df_meas, column, desc, station, from_dt, to_dt)
        return jsonify(result)
    else:
        return "You should never see this, but contact the webmaster if you do."

@app.route("/api/v1.0/temp")
def temp():
    column = 'temp'
    desc = 'temperature'
    df_meas, df_sta = make_connection()
    (error, qtype, station, from_dt, to_dt) = valid_query(request.args, df_meas, df_sta)
    if error:
        return error
    elif qtype == 'stats':
        result = get_stats(df_meas, column, desc, station, from_dt, to_dt)
        return jsonify(result)
    elif qtype == 'data':
        result = get_data(df_meas, column, desc, station, from_dt, to_dt)
        return jsonify(result)
    else:
        return "You should never see this, but contact the webmaster if you do."

if __name__ == "__main__":
    app.run(debug=True)

