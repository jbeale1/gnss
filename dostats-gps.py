#!/usr/bin/env python3

# Compute statistics from NMEA GPS / GNSS log files
# shows stats for separate 15-minute blocks
# J.Beale 2025-07-04

import pynmea2
import math
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict

def convert_to_decimal(coord, direction):
    degrees = int(float(coord) / 100)
    minutes = float(coord) - degrees * 100
    decimal = degrees + minutes / 60
    return decimal if direction in ('N', 'E') else -decimal

def haversine(lat1, lon1, lat2, lon2):
    R = 6371e3
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def safe_stats(values):
    if not values:
        return (np.nan, np.nan, np.nan)
    return (
        float(sum(values)) / len(values),
        float(min(values)),
        float(max(values))
    )

def parse_nmea_log(file_path):
    from collections import defaultdict
    blocks = defaultdict(list)
    snrs_by_block = defaultdict(list)
    sat_counts_by_block = defaultdict(list)
    hdops_by_block = defaultdict(list)
    vdops_by_block = defaultdict(list)
    last_timestamp = None
    current_utc_date = None
    gpgga_buffer = []

    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()

            if line.startswith('$GPRMC') or line.startswith('$GNRMC'):
                try:
                    parts = line.split(',')
                    if len(parts) >= 10:
                        date_str = parts[9]  # DDMMYY
                        day = int(date_str[0:2])
                        month = int(date_str[2:4])
                        year = int(date_str[4:6]) + 2000
                        current_utc_date = datetime(year, month, day)
                        # Process any buffered GPGGA lines now that we have the date
                        for gpgga_line in gpgga_buffer:
                            try:
                                msg = pynmea2.parse(gpgga_line)
                                dt = datetime.combine(current_utc_date.date(), msg.timestamp, tzinfo=timezone.utc)
                                last_timestamp = dt
                                minute_block = (dt.minute // 15) * 15
                                block_start = dt.replace(minute=minute_block, second=0, microsecond=0, tzinfo=timezone.utc)
                                lat = convert_to_decimal(msg.lat, msg.lat_dir)
                                lon = convert_to_decimal(msg.lon, msg.lon_dir)
                                alt = float(msg.altitude)
                                blocks[block_start].append((lat, lon, alt))
                            except Exception:
                                continue
                        gpgga_buffer = []
                except:
                    continue

            elif line.startswith('$GPGGA') or line.startswith('$GNGGA'):
                if not current_utc_date:
                    gpgga_buffer.append(line)
                    continue  # skip until date is known
                try:
                    msg = pynmea2.parse(line)
                    dt = datetime.combine(current_utc_date.date(), msg.timestamp, tzinfo=timezone.utc)
                    last_timestamp = dt
                    minute_block = (dt.minute // 15) * 15
                    block_start = dt.replace(minute=minute_block, second=0, microsecond=0, tzinfo=timezone.utc)
                    lat = convert_to_decimal(msg.lat, msg.lat_dir)
                    lon = convert_to_decimal(msg.lon, msg.lon_dir)
                    alt = float(msg.altitude)
                    blocks[block_start].append((lat, lon, alt))
                except Exception:
                    continue

            elif line.startswith('$GPGSV') and last_timestamp:
                parts = line.split(',')
                minute_block = (last_timestamp.minute // 15) * 15
                block_start = last_timestamp.replace(minute=minute_block, second=0, microsecond=0)
                try:
                    if len(parts) >= 4 and parts[2] == '1':
                        sat_counts_by_block[block_start].append(int(parts[3]))
                    for i in range(7, len(parts), 4):
                        try:
                            snr = int(parts[i])
                            snrs_by_block[block_start].append(snr)
                        except:
                            continue
                except:
                    continue

            elif (line.startswith('$GPGSA') or line.startswith('$GNGSA')) and last_timestamp:
                parts = line.split(',')
                minute_block = (last_timestamp.minute // 15) * 15
                block_start = last_timestamp.replace(minute=minute_block, second=0, microsecond=0)
                try:
                    if len(parts) >= 18:
                        hdop = float(parts[16])
                        vdop = float(parts[17].split('*')[0])
                        hdops_by_block[block_start].append(hdop)
                        vdops_by_block[block_start].append(vdop)
                except:
                    continue

    return blocks, sat_counts_by_block, snrs_by_block, hdops_by_block, vdops_by_block

def calculate_block_stats(lat_lon_alt_data, sat_counts, snrs, hdops, vdops):
    lats, lons, alts = zip(*lat_lon_alt_data) if lat_lon_alt_data else ([], [], [], [])

    if not lats:
        return {}

    avg_lat = sum(lats) / len(lats)
    avg_lon = sum(lons) / len(lons)
    avg_alt = sum(alts) / len(alts)

    horizontal_devs = [haversine(lat, lon, avg_lat, avg_lon) for lat, lon in zip(lats, lons)]
    horiz_std = (sum((d - sum(horizontal_devs)/len(horizontal_devs))**2 for d in horizontal_devs) / len(horizontal_devs))**0.5
    horiz_max = max(horizontal_devs)

    alt_std = (sum((a - avg_alt)**2 for a in alts) / len(alts))**0.5
    alt_range = max(alts) - min(alts)

    snr_mean, snr_min, snr_max = safe_stats(snrs)
    sat_mean, sat_min, sat_max = safe_stats(sat_counts)
    hdop_mean, hdop_min, hdop_max = safe_stats(hdops)
    vdop_mean, vdop_min, vdop_max = safe_stats(vdops)

    return {
        'lat': avg_lat,
        'lon': avg_lon,
        'z_m': avg_alt,
        'z_SD': alt_std,
        'zRng_m': alt_range,
        'Hdev_SD': horiz_std,
        'Hdev_m': horiz_max,
        'Npts': len(lat_lon_alt_data),
        'SV_av': sat_mean,
        'SV_min': sat_min,
        'SV_max': sat_max,
        'snr_av': snr_mean,
        'smin': snr_min,
        'smax': snr_max,
        'vdop': vdop_mean,
        'vd_min': vdop_min,
        'vd_max': vdop_max
    }


def showMeans(df):
    # ---- Altitude Quality Analysis ----

    # Simple mean of all altitude values
    simple_mean = df['z_m'].mean()

    # Filter for high-quality data
    filtered_df = df[(df['z_SD'] < 1.5) & (df['vdop'] < 2.0) & (df['snr_av'] > 35)]
    filtered_mean = filtered_df['z_m'].mean()

    # Filtered Mean Latitude and Longitude
    filtered_mean_lat = filtered_df['lat'].mean()
    filtered_mean_lon = filtered_df['lon'].mean()

    # Weighted mean by 1 / z_SD (only positive z_SD)
    valid_zsd = df['z_SD'] > 0
    weighted_zsd = (df.loc[valid_zsd, 'z_m'] / df.loc[valid_zsd, 'z_SD']).sum() / (1 / df.loc[valid_zsd, 'z_SD']).sum()

    # Weighted mean by 1 / vdop (only positive vdop)
    valid_vdop = df['vdop'] > 0
    weighted_vdop = (df.loc[valid_vdop, 'z_m'] / df.loc[valid_vdop, 'vdop']).sum() / (1 / df.loc[valid_vdop, 'vdop']).sum()

    # Fraction of data included in filtered mean
    filtered_fraction = len(filtered_df) / len(df)

    # Only consider rows with valid (positive) z_SD and vdop to avoid division by zero
    valid_rows = df[(df['z_SD'] > 0) & (df['vdop'] > 0)].copy()

    # Calculate weights
    valid_rows['weight_zsd'] = 1 / valid_rows['z_SD']
    valid_rows['weight_vdop'] = 1 / valid_rows['vdop']

    # Normalize the weights so their sums equal 1
    valid_rows['weight_zsd_norm'] = valid_rows['weight_zsd'] / valid_rows['weight_zsd'].sum()
    valid_rows['weight_vdop_norm'] = valid_rows['weight_vdop'] / valid_rows['weight_vdop'].sum()

    # Compute Pearson correlation coefficient
    correlation = valid_rows[['weight_zsd_norm', 'weight_vdop_norm']].corr().iloc[0, 1]

    estimates = [simple_mean, filtered_mean, weighted_zsd, weighted_vdop]
    diff = max(estimates) - min(estimates)
    total = valid_rows['Npts'].sum()

    # Return comment lines to optionally write to file
    comment_lines = [
        f"# Altitude Estimates:",
        f"# Simple Mean Altitude:       {simple_mean:7.2f} m",
        f"# Filtered Mean Altitude:     {filtered_mean:7.2f} m",
        f"# Filtered Mean Latitude:     {filtered_mean_lat:10.6f}",
        f"# Filtered Mean Longitude:    {filtered_mean_lon:10.6f}",
        f"# Weighted Mean (1/z_SD):     {weighted_zsd:7.2f} m",
        f"# Weighted Mean (1/vdop):     {weighted_vdop:7.2f} m",
        f"# Max diff of these 4:        {diff:7.2f} m",
        f"# Fraction of data used:      {filtered_fraction:7.1%}",
        f"# Wgt. Corr. (z_SD vs vdop):  {correlation:7.4f}",
        f"# Total points (= seconds):    {total}"
    ]
    return comment_lines


# =============================================================================
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} input_file")
        sys.exit(1)

    input_file = sys.argv[1]
    blocks, sats, snrs, hdops, vdops = parse_nmea_log(input_file)

    rows = []
    for block in sorted(blocks):
        stats = calculate_block_stats(
            blocks[block],
            sats.get(block, []),
            snrs.get(block, []),
            hdops.get(block, []),
            vdops.get(block, [])
        )
        stats['block_start_utc'] = block.strftime('%Y-%m-%d %H:%M:%S')
        rows.append(stats)

    df = pd.DataFrame(rows)
    pd.set_option('display.precision', 7)
    df['z_m'] = df['z_m'].round(2)
    df['z_SD'] = df['z_SD'].round(3)
    df['Hdev_m'] = df['Hdev_m'].round(5)
    df['Hdev_SD'] = df['Hdev_SD'].round(5)
    df['snr_av'] = df['snr_av'].round(2)
    df['SV_av'] = df['SV_av'].round(2)
    df['vdop'] = df['vdop'].round(2)
    
    numeric_cols = [
        'lat', 'lon', 'z_m', 'z_SD', 'zRng_m', 'Hdev_SD', 'Hdev_m',
        'Npts', 'SV_av', 'SV_min', 'SV_max', 'snr_av', 'smin', 'smax',
        'vdop', 'vd_min', 'vd_max'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print(df.to_string(index=False))
    print()


    showMeans(df) # show best estimates of altitude
    fout = input_file[:-4] + "_stats.csv"
    
    # Write CSV with comment header
    comment_lines = showMeans(df)  # Also prints to screen
    with open(fout, 'w') as f:
        for line in comment_lines:
            print(line)
            f.write(line + "\n")
        df.to_csv(f, index=False)
    
    print(f"\nStatistics written to {fout}")

