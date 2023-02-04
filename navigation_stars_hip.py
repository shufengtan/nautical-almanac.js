from skyfield.api import load
from skyfield.data import hipparcos
import re
import pandas as pd
import requests

def load_hipparcos():
    with load.open(hipparcos.URL) as f:
        return hipparcos.load_dataframe(f)

def read_wiki_navigation_stars_table(html=None):
    if html is None:
        html = requests.get('https://en.wikipedia.org/wiki/List_of_stars_for_navigation').text
    re_IS = re.I + re.S
    remove_tags = lambda x: re.sub(r'</?(a|b|br|span|sup|sub)\b[^>]*>', ' ', x).strip().rstrip()
    for table in re.findall(r'<table\b[^>]*>(.*?)</table>', html, re_IS):
        headers = []
        rows = []
        for row in re.findall(r'<tr\b[^>]*>(.*?)</tr>', table, re_IS):
            cols = [remove_tags(c) for c in re.findall(r'<t[dh]\b[^>]*>(.*?)</t[dh]>', row, re_IS)]
            if len(cols) == 0:
                continue
            rows.append(cols)
        if len(rows) >= 50:
            col_indices = [idx for idx,h in enumerate(rows[0]) if re.search(r'(name|Bayer|SHA|Dec|magnitude)', h)]
            headers = [rows[0][j] for j in col_indices]
            data = []
            for row in rows[1:]:
                if row[0] == '-100':
                    continue
                idx_name = rows[0].index('Common name')
                row[idx_name] = re.sub(r'\s*&#\d+;.*', '', row[idx_name])
                idx_bayer = rows[0].index('Bayer designation')
                bayer_des = row[idx_bayer].split()
                row[idx_bayer] = ' '.join(bayer_des[:len(bayer_des)//2])
                idx_dec = rows[0].index('Declination')
                row[idx_dec] = float(row[idx_dec].split(' ')[0])
                idx_mag = rows[0].index('App. magnitude')
                row[idx_mag] = float(re.sub(r' *<small>var.*', '', row[idx_mag].replace('&#8722;', '-')))
                data.append([row[j] for j in col_indices])
            return pd.DataFrame(data, columns=headers).sort_values('App. magnitude').reset_index()

def search_hip(df, ra, dec, mag):
    ra_diff = df['ra_degrees'] - ra
    dec_diff = df['dec_degrees'] - dec
    mag_diff = df['magnitude'] - mag
    df['SSE'] = ra_diff*ra_diff + dec_diff*dec_diff + mag_diff*mag_diff
    df2 = df[df['SSE'] < 1]
    if df2.shape[0] == 0:
        df2 = df.sort_values('SSE')
    if df2.shape[0] == 1 or df2.iloc[1]['SSE'] > 10*df2.iloc[0]['SSE']:
        return df2.index[0], list(df2.iloc[0][['SSE', 'ra_degrees', 'dec_degrees', 'magnitude']])
    return None, None

def get_hip_dataframe(df_navigation):
    headers = ['Common name', 'Bayer designation', 'Hip', 'SSE', 'Wiki RA', 'Hip RA', 'Wiki dec', 'Hip dec', 'Wiki mag', 'Hip mag']
    rows = []
    for idx in df_navigation.index:
        z = df_navigation.iloc[idx]
        ra = 360 - float(z['SHA'])
        dec = z['Declination']
        mag = z['App. magnitude']
        hip, res = search_hip(df_bright, ra, dec,mag)
        rows.append([z[headers[0]], z[headers[1]], hip, res[0], ra, res[1], dec, res[2], mag, res[3]])
    return pd.DataFrame(rows, columns=headers)

if __name__ == '__main__':
    df = load_hipparcos()

    df_bright = df[df.magnitude <= 3.5].sort_values('magnitude')
    print(f'Loaded {df_bright.shape[0]} bright stars from hipparcos')

    df_navigation = read_wiki_navigation_stars_table()
    print(f'Read {df_navigation.shape[0]} navigation stars from wiki table')

    df_nav_star_hip = get_hip_dataframe(df_navigation)
    df_nav_star_hip.to_csv('nav_star_hip.csv', index=False)
