#!/usr/bin/python

# log NMEA data from GPS/GNSS receiver to a file, creating a new file each day.
# J.beale 2025-07-04

import serial
from datetime import datetime, timezone
import os
import time

# Configuration
# serial_port = '/dev/ttyACM0'
serial_port = '/dev/ttyUSB0'
baud_rate = 115200  # assuming the GPS module uses 9600 baud rate
#outdir = r"/home/pi/gps"
outdir = r"/home/jbeale/Documents/gps"

def get_log_filename(dt):
    return f"NS_log_{dt.strftime('%Y%m%d_%H%M%S')}.txt"

def main():
    try:
        with serial.Serial(serial_port, baud_rate, timeout=1) as ser:
            current_day = None
            log_file = None
            log_filename = None

            while True:
                # Get current UTC date
                now = datetime.now(timezone.utc)
                day = now.date()

                # If it's a new day or no file is open, open a new file
                if (current_day != day) or (log_file is None):
                    if log_file:
                        log_file.close()
                        print(f"Closed log file: {log_filename}")
                    log_filename = get_log_filename(now)
                    log_path = os.path.join(outdir, log_filename)
                    log_file = open(log_path, 'w')
                    print(f"Logging GPS data from {serial_port} to {log_filename}...")
                    current_day = day

                line = ser.readline().decode(errors='replace').strip()
                if line:
                    log_line = f"{line}\n"
                    log_file.write(log_line)
                    log_file.flush()
                    print(log_line, end='')  # Optional: show on console too

    except serial.SerialException as e:
        print(f"Error opening serial port: {e}")
    except KeyboardInterrupt:
        print("\nLogging stopped by user.")
    finally:
        if 'log_file' in locals() and log_file:
            log_file.close()
            print(f"Closed log file: {log_filename}")

if __name__ == "__main__":
    main()
