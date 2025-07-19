#!/usr/bin/env python3

# Compute statistics from NMEA GPS / GNSS log files
# shows stats for separate 15-minute blocks (or other N)
# J.Beale 2025-07-14

import pynmea2
import math
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os

VERSION = "DoStats-GPS v1.57 2025-07-18"

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


def get_block_start(dt, block_size):
    """Return the datetime rounded down to the nearest block_size minutes."""
    total_minutes = dt.hour * 60 + dt.minute
    block_start_minutes = (total_minutes // block_size) * block_size
    block_hour = block_start_minutes // 60
    block_minute = block_start_minutes % 60
    return dt.replace(hour=block_hour, minute=block_minute, second=0, microsecond=0, tzinfo=dt.tzinfo)

def parse_nmea_log(file_path, block_size=15):
    from collections import defaultdict
    blocks = defaultdict(list)
    snrs_by_block = defaultdict(list)
    sat_counts_by_block = defaultdict(list)
    hdops_by_block = defaultdict(list)
    vdops_by_block = defaultdict(list)
    prns_by_block = defaultdict(set)
    last_timestamp = None
    current_utc_date = None
    gpgga_buffer = []
    seen_timestamps = set()  # track possible duplicates

    with open(file_path, 'rb') as f:  # Open in binary mode
        for raw_line in f:
            try:
                line = raw_line.decode('ascii', errors='strict').strip()
            except UnicodeDecodeError:
                continue  # Skip binary or non-ASCII lines

            if not line.startswith('$'):
                continue  # Skip non-NMEA lines

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

                                timestamp_key = dt.strftime('%H:%M:%S')
                                if timestamp_key not in seen_timestamps:
                                    seen_timestamps.add(timestamp_key)
                                    block_start = get_block_start(dt, block_size)
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

                    timestamp_key = dt.strftime('%H:%M:%S')
                    if timestamp_key not in seen_timestamps:
                        seen_timestamps.add(timestamp_key)
                        block_start = get_block_start(dt, block_size)
                        lat = convert_to_decimal(msg.lat, msg.lat_dir)
                        lon = convert_to_decimal(msg.lon, msg.lon_dir)
                        alt = float(msg.altitude)
                        blocks[block_start].append((lat, lon, alt))
                except Exception:
                    continue

            elif (line.startswith('$GPGSV') or 
                  line.startswith('$GLGSV') or 
                  line.startswith('$GAGSV') or 
                  line.startswith('$BDGSV') or 
                  line.startswith('$GNGSV')) and last_timestamp:
                parts = line.split(',')
                block_start = get_block_start(last_timestamp, block_size)
                try:
                    # Each GSV sentence: PRNs are at positions 4, 8, 12, 16
                    for i in [4, 8, 12, 16]:
                        if i < len(parts) and parts[i].isdigit():
                            prns_by_block[block_start].add(int(parts[i]))

                    # validation for GPGSV sentences
                    if (len(parts) >= 4 and
                        parts[2] == '1' and  # First sentence in group
                        parts[1].isdigit() and  # Total sentences should be numeric
                        parts[3].isdigit() and  # Satellite count should be numeric
                        1 <= int(parts[1]) <= 9 and  # Reasonable total sentences (1-9)
                        0 <= int(parts[3]) <= 50):   # Reasonable satellite count (0-50)

                        sat_counts_by_block[block_start].append(int(parts[3]))

                    # Parse SNR values with validation
                    for i in range(7, len(parts), 4):
                        if i < len(parts) and parts[i].isdigit():
                            try:
                                snr = int(parts[i])
                                if 0 <= snr <= 99:  # Valid SNR range
                                    snrs_by_block[block_start].append(snr)
                            except (ValueError, IndexError):
                                continue
                except (ValueError, IndexError):
                    continue

            elif (line.startswith('$GPGSA') or line.startswith('$GNGSA')) and last_timestamp:
                parts = line.split(',')
                block_start = get_block_start(last_timestamp, block_size)
                try:
                    if len(parts) >= 18:
                        hdop = float(parts[16])
                        vdop = float(parts[17].split('*')[0])
                        hdops_by_block[block_start].append(hdop)
                        vdops_by_block[block_start].append(vdop)
                except:
                    continue

    return blocks, sat_counts_by_block, snrs_by_block, hdops_by_block, vdops_by_block, prns_by_block

def calculate_block_stats(lat_lon_alt_data, sat_counts, snrs, hdops, vdops, prns):
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

    # Use the number of unique PRNs seen in the block as the true SV count
    sv_count = len(prns) if prns is not None else np.nan

    return {
        'lat': avg_lat,
        'lon': avg_lon,
        'z_m': avg_alt,
        'z_SD': alt_std,
        'zRng_m': alt_range,
        'Hdev_SD': horiz_std,
        'Hdev_m': horiz_max,
        'Npts': len(lat_lon_alt_data),
        'SV_count': sv_count,  # <-- new field for unique satellites
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


def showMeans(df, date_str, block_size):
    # ---- Altitude Quality Analysis ----

    # Simple mean of all altitude values
    simple_mean = df['z_m'].mean()

    # Filter based on hardcoded threshold for high-quality data
    # filtered_df = df[(df['z_SD'] < 1.5) & (df['vdop'] < 2.0) & (df['snr_av'] > 35)]
    # filtered_df = df[(df['z_SD'] < 1.35) & (df['vdop'] < 2.0) & (df['snr_av'] > 35)]

    # Compute adaptive thresholds (Nth percentile for each)
    z_sd_thresh = df['z_SD'].quantile(0.85)
    vdop_thresh = df['vdop'].quantile(0.85)
    snr_av_thresh = df['snr_av'].quantile(0.1)

    # Filter out rows with the worst stats on any parameter
    filtered_df = df[
        (df['z_SD'] < z_sd_thresh) &
        (df['vdop'] < vdop_thresh) &
        (df['snr_av'] > snr_av_thresh)
    ]
    # If no rows remain after filtering, return empty comment lines
    if filtered_df.empty:
        return [
            f"# DATE,LAT,LON,MSL,DEV,SNR,DAYS,BLKSIZE",
            f"## {date_str}, No valid data after filtering with block size {block_size} minutes."
        ]


    filtered_mean = filtered_df['z_m'].mean()
    filtered_snr_av = filtered_df['snr_av'].mean()

    # Filtered Mean Latitude and Longitude
    filtered_mean_lat = filtered_df['lat'].mean()
    filtered_mean_lon = filtered_df['lon'].mean()

    # Weighted mean by 1 / z_SD (only positive z_SD)
    valid_zsd = df['z_SD'] > 0
    weighted_zsd = (df.loc[valid_zsd, 'z_m'] / df.loc[valid_zsd, 'z_SD']).sum() / (1 / df.loc[valid_zsd, 'z_SD']).sum()

    # Weighted mean by 1 / vdop (only positive vdop)
    valid_vdop = df['vdop'] > 0
    weighted_vdop = (df.loc[valid_vdop, 'z_m'] / df.loc[valid_vdop, 'vdop']).sum() / (1 / df.loc[valid_vdop, 'vdop']).sum()

    # --- New: Weighted mean for top 50% snr_av blocks ---
    snr_sorted = df.sort_values('snr_av', ascending=False)
    n_top = max(1, len(snr_sorted) // 2)
    top_snr = snr_sorted.iloc[:n_top]
    weighted_snr = np.nan
    if not top_snr.empty and (top_snr['snr_av'] > 0).any():
        weights = top_snr['snr_av']
        weighted_snr = (top_snr['z_m'] * weights).sum() / weights.sum()

    # Fraction of data included in filtered mean
    filtered_fraction = len(filtered_df) / len(df)

    # Calculate average SV_count for filtered blocks
    filtered_sv_count = filtered_df['SV_count'].mean() if 'SV_count' in filtered_df.columns else np.nan

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

    # --- Add weighted_snr to the estimates and diff calculation ---
    estimates = [simple_mean, filtered_mean, weighted_zsd, weighted_vdop, weighted_snr]
    diff = max(estimates) - min(estimates)
    total = valid_rows['Npts'].sum()
    days = total / (60*60*24)

    # Return comment lines to optionally write to file
    comment_lines = [
        f"# DATE,LAT,LON,MSL,DEV,SNR,DAYS,BLKSIZE",
        f"## {date_str}, {filtered_mean_lat:10.7f}, {filtered_mean_lon:10.7f}, {filtered_mean:6.2f}, {diff:5.3f}, {filtered_snr_av:.2f}, {days:5.3f}, {block_size}",
        f"# Simple Mean Altitude:       {simple_mean:7.3f} m",
        f"# Filtered Mean Altitude:     {filtered_mean:7.3f} m",
        f"# Weighted Mean (1/z_SD):     {weighted_zsd:7.3f} m",
        f"# Weighted Mean (1/vdop):     {weighted_vdop:7.3f} m",
        f"# Weighted Mean (top 50% SNR):{weighted_snr:7.3f} m",
        f"# Max diff of these 5:        {diff:7.3f} m",
        f"# Fraction of data used:      {filtered_fraction:7.1%} ({block_size})",
        f"# SV count (filtered):        {filtered_sv_count:.2f}",
        f"# Avg snr_av (filtered):      {filtered_snr_av:7.2f}",
        f"# Wgt. Corr. (z_SD vs vdop):  {correlation:7.4f}",
        f"# Seconds (days):              {total} ({days:5.3f})"
    ]
    return comment_lines


# =============================================================================
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} input_file [-nd] [-bs <block_size_minutes>] [-ub] [-ubs]")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        print(f"Error: input file '{input_file}' does not exist.")
        sys.exit(1)

    no_details = "-nd" in sys.argv
    ultra_brief = "-ub" in sys.argv
    ultra_brief_set = "-ubs" in sys.argv

    # Handle -ubs (ultra brief set): do range of block sizes, print only the "##" summary line for each
    if ultra_brief_set:
        for bs in [10, 15, 20, 25, 30, 60, 120, 240]:
            blocks, sats, snrs, hdops, vdops, prns_by_block = parse_nmea_log(input_file, bs)
            rows = []
            for block in sorted(blocks):
                stats = calculate_block_stats(
                    blocks[block],
                    sats.get(block, []),
                    snrs.get(block, []),
                    hdops.get(block, []),
                    vdops.get(block, []),
                    prns_by_block.get(block, set())
                )
                if stats:
                    stats['block_start_utc'] = block.strftime('%Y-%m-%d %H:%M:%S')
                    rows.append(stats)
            df = pd.DataFrame(rows)
            if df.empty or 'z_m' not in df.columns:
                continue
            first_date = df['block_start_utc'].iloc[0][:10].replace('-', '')
            comment_lines = showMeans(df, first_date, bs)
            for line in comment_lines:
                if line.startswith("##"):
                    print(line)
                    break
        sys.exit(0)

    block_size = 15
    if "-bs" in sys.argv:
        try:
            idx = sys.argv.index("-bs")
            block_size = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            print("Usage: -bs <block_size_minutes>")
            sys.exit(1)

    if not ultra_brief:
        print(VERSION)
    blocks, sats, snrs, hdops, vdops, prns_by_block = parse_nmea_log(input_file, block_size)

    rows = []
    for block in sorted(blocks):
        stats = calculate_block_stats(
            blocks[block],
            sats.get(block, []),
            snrs.get(block, []),
            hdops.get(block, []),
            vdops.get(block, []),
            prns_by_block.get(block, set())
        )
        if stats:  # Only append if stats is not empty
            stats['block_start_utc'] = block.strftime('%Y-%m-%d %H:%M:%S')
            rows.append(stats)

    df = pd.DataFrame(rows)

    if df.empty or 'z_m' not in df.columns:
        print("No valid data found in input file.")
        sys.exit(1)

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

    first_date = df['block_start_utc'].iloc[0][:10].replace('-', '')

    comment_lines = showMeans(df, first_date, block_size)
    fout = input_file[:-4] + "_stats.csv"
    
    if ultra_brief:
        # Only print the line starting with "##"
        for line in comment_lines:
            if line.startswith("##"):
                print(line)
                break
        sys.exit(0)

    if not no_details:
        print(df.to_string(index=False))
        print()

    # Write CSV with comment header
    with open(fout, 'w') as f:
        for line in comment_lines:
            print(line)
            f.write(line + "\n")
        df.to_csv(f, index=False)
    
    print(f"\nStatistics written to {fout}")
