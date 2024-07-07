import pandas as pd
import requests
import logging

def fetch_pv(pv,time_start,time_end,
             url='http://kekb-co-web.kek.jp/archappl_skekb/retrieval/data/getData.json',
             columns=['time','val','unixtime_ns'],
             within_timerange=True,
            ):
    """
Fetch PV from archiver and return DataFrame
----------
Parameters
----------
pv        : name of the PV in archiver
time_start: start time as datetime with tzinfo
time_end  : end time as datetime with tzinfo
url: json URL for the archiver

the different URL options are
http://kekb-co-web.kek.jp/archappl_skekb/retrieval/data/getData.json # SuperKEKB, works!
http://kekb-co-web.kek.jp/archappl_skekb/retrieval/data/getData.qw   # works but no nanos
http://kekb-co-web.kek.jp/archappl_skekb/retrieval/data/getData.raw  # from Kaji, unable to parse to json
http://kekb-co-adm03.kek.jp/archappl_skekb/retrieval/data/getData.json
http://130.87.83.238/archappl_skekb/retrieval/data/getData.json
http://172.22.16.120:17668/retrieval/data/getData.json               # Belle2, works!
    """
    logging.debug('contributor: Krishanu Bhattacharyya (https://github.com/krish1010)')
    time_start_str = time_start.isoformat() #ISO 8601 format, UTC timezone
    time_end_str   = time_end.isoformat()   #ISO 8601 format, UTC timezone
    logging.debug(f'fetching {pv} data for {url}')
    params = {
        'pv'     : pv,
        'from'   : time_start_str,
        'to'     : time_end_str,
        'fetchLatestMetadata': 'true',
    }
    resp = requests.request(
        method='GET',
        url=url,
        params=params,
    )
    if resp.status_code != 200 or len(resp.json())==0 or len(resp.json()[0]['data'])==0:
        logging.error(f"no data was fetched for PV {pv} from archiver")
        return pd.DataFrame(columns=columns)
    logging.debug(f"fetched {len(resp.json()[0]['data'])} entries")
    
    df=pd.DataFrame(resp.json()[0]['data'])
    # df['time']=[pd.to_datetime(row[1]['secs']*1e9+row[1]['nanos'],utc=True).tz_convert('Asia/Tokyo') for row in df.iterrows()]
    df['time']=pd.to_datetime(df['secs']*1e9+df['nanos'],utc=True) 
    df['unixtime_ns']=df['secs']*int(1e9)+df['nanos'] #time is accurate!
    logging.debug(f"start : {df['time'].min()}")
    logging.debug(f"end   : {df['time'].max()}")
    df_within_timerange = df[(df['time']>=time_start) & (df['time']<=time_end)]

    if len(df)==1:
        logging.warning(f"only one data point is fetched for PV {pv} from archiver")

    if len(df_within_timerange)!=len(df):
        logging.warning(f"Archiver returned {len(df)-len(df_within_timerange)} data point(s) for PV {pv} outside time range. They might be dropped.")

    if within_timerange:
        return df_within_timerange[columns]
    else:
        return df[columns]
