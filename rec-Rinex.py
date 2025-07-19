#!/usr/bin/env python3
"""
u-blox ZED-F9P GNSS Raw Data to RINEX Converter

This program connects to a u-blox ZED-F9P GNSS receiver, collects raw
measurement data, and writes it to a RINEX observation file.

Requirements:
- pyserial
- numpy
- datetime

Usage:
    python ublox_rinex_recorder.py --port /dev/ttyUSB0 --duration 3600

works on Win10.  JBeale 7/19/2025 
"""

import serial
import struct
import time
import datetime
import argparse
import sys
from collections import defaultdict
import math

class UBXMessage:
    """UBX message parser for u-blox receivers"""
    
    # UBX message classes and IDs
    UBX_RXM_RAWX = (0x02, 0x15)  # Raw measurement data
    UBX_NAV_TIMEGPS = (0x01, 0x20)  # GPS time solution
    UBX_CFG_MSG = (0x06, 0x01)  # Message configuration
    
    def __init__(self):
        self.sync1 = 0xB5
        self.sync2 = 0x62
        
    def calculate_checksum(self, data):
        """Calculate UBX checksum"""
        ck_a = ck_b = 0
        for byte in data:
            ck_a = (ck_a + byte) & 0xFF
            ck_b = (ck_b + ck_a) & 0xFF
        return ck_a, ck_b
    
    def create_message(self, msg_class, msg_id, payload=b''):
        """Create a UBX message with proper framing and checksum"""
        header = struct.pack('<BBBB', self.sync1, self.sync2, msg_class, msg_id)
        length = struct.pack('<H', len(payload))
        data = header + length + payload
        ck_a, ck_b = self.calculate_checksum(data[2:])
        return data + struct.pack('<BB', ck_a, ck_b)
    
    def parse_message(self, data):
        """Parse a complete UBX message"""
        if len(data) < 8:
            return None
            
        sync1, sync2, msg_class, msg_id, length = struct.unpack('<BBBBH', data[:6])
        
        if sync1 != self.sync1 or sync2 != self.sync2:
            return None
            
        if len(data) < 8 + length:
            return None
            
        payload = data[6:6+length]
        ck_a, ck_b = data[6+length:8+length]
        
        # Verify checksum
        calc_ck_a, calc_ck_b = self.calculate_checksum(data[2:6+length])
        if ck_a != calc_ck_a or ck_b != calc_ck_b:
            return None
            
        return {
            'class': msg_class,
            'id': msg_id,
            'length': length,
            'payload': payload
        }

class RINEXObservationFile:
    """RINEX observation file writer"""
    
    def __init__(self, filename, station_name="UNKN", receiver_type="u-blox ZED-F9P"):
        self.filename = filename
        self.station_name = station_name
        self.receiver_type = receiver_type
        self.file = None
        self.first_obs_time = None
        self.obs_types = {
            'G': ['C1C', 'L1C', 'D1C', 'S1C', 'C2W', 'L2W', 'D2W', 'S2W'],  # GPS
            'R': ['C1C', 'L1C', 'D1C', 'S1C', 'C2C', 'L2C', 'D2C', 'S2C'],  # GLONASS
            'E': ['C1C', 'L1C', 'D1C', 'S1C', 'C5Q', 'L5Q', 'D5Q', 'S5Q'],  # Galileo
            'C': ['C2I', 'L2I', 'D2I', 'S2I', 'C6I', 'L6I', 'D6I', 'S6I'],  # BeiDou
        }
        
    def open_file(self):
        """Open RINEX file and write header"""
        self.file = open(self.filename, 'w')
        self._write_header()
        
    def _write_header(self):
        """Write RINEX observation file header"""
        # RINEX version and file type
        self.file.write(f"     3.04           OBSERVATION DATA    M                   RINEX VERSION / TYPE\n")
        
        # Program and run by
        self.file.write(f"Python UBX2RINEX   User                {datetime.datetime.utcnow().strftime('%Y%m%d %H%M%S')} UTC PGM / RUN BY / DATE\n")
        
        # Marker name
        self.file.write(f"{self.station_name:<60}MARKER NAME\n")
        
        # Observer and agency
        self.file.write(f"Observer            Agency              OBSERVER / AGENCY\n")
        
        # Receiver info
        self.file.write(f"{self.receiver_type:<20}Unknown         Unknown             REC # / TYPE / VERS\n")
        
        # Antenna info
        self.file.write(f"Unknown             Unknown                                 ANT # / TYPE\n")
        
        # Approximate position (placeholder)        
        self.file.write(f"  -1,000,000   -1,000,000   2,000,000.500            APPROX POSITION XYZ\n")
        
        # Antenna delta (placeholder)
        self.file.write(f"        0.0000        0.0000        0.0000                  ANTENNA: DELTA H/E/N\n")
        
        # System and observation types
        for sys, obs_list in self.obs_types.items():
            obs_count = len(obs_list)
            obs_line = f"{sys}  {obs_count:3d}"
            for i, obs in enumerate(obs_list):
                if i % 13 == 0 and i > 0:
                    obs_line += f"      SYS / # / OBS TYPES\n{' '*6}"
                obs_line += f" {obs}"
            obs_line += " " * (60 - len(obs_line)) + "SYS / # / OBS TYPES\n"
            self.file.write(obs_line)
        
        # Time of first observation (will be updated)
        self.file.write(f"  2025     1     1     0     0    0.0000000     GPS         TIME OF FIRST OBS\n")
        
        # End of header
        self.file.write(f"{'END OF HEADER':<60}END OF HEADER\n")
        
    def write_observation_epoch(self, gps_time, satellites, observations):
        """Write observation epoch to RINEX file"""
        if not self.file:
            return
            
        # Convert GPS time to calendar time
        gps_epoch = datetime.datetime(1980, 1, 6)
        obs_time = gps_epoch + datetime.timedelta(seconds=gps_time)
        
        if self.first_obs_time is None:
            self.first_obs_time = obs_time
            
        # Write epoch header
        sat_count = len(satellites)
        self.file.write(f"> {obs_time.year:4d} {obs_time.month:2d} {obs_time.day:2d} "
                       f"{obs_time.hour:2d} {obs_time.minute:2d} {obs_time.second:11.7f}  "
                       f"0{sat_count:3d}\n")
        
        # Write observations for each satellite
        for sat_id in satellites:
            if sat_id in observations:
                obs_data = observations[sat_id]
                sys_char = self._get_system_char(sat_id)
                sat_num = sat_id % 100
                
                obs_line = f"{sys_char}{sat_num:02d}"
                
                # Write observations in the order defined in obs_types
                if sys_char in self.obs_types:
                    for obs_type in self.obs_types[sys_char]:
                        if obs_type in obs_data:
                            value = obs_data[obs_type]['value']
                            lli = obs_data[obs_type].get('lli', 0)
                            strength = obs_data[obs_type].get('strength', 0)
                            obs_line += f"{value:14.3f}{lli:1d}{strength:1d}"
                        else:
                            obs_line += "                "
                
                obs_line += "\n"
                self.file.write(obs_line)
                
    def _get_system_char(self, sat_id):
        """Get GNSS system character from satellite ID"""
        if sat_id <= 32:
            return 'G'  # GPS
        elif 65 <= sat_id <= 96:
            return 'R'  # GLONASS
        elif 211 <= sat_id <= 246:
            return 'E'  # Galileo
        elif 159 <= sat_id <= 194:
            return 'C'  # BeiDou
        else:
            return 'G'  # Default to GPS
            
    def close_file(self):
        """Close RINEX file"""
        if self.file:
            self.file.close()

class UBloxRINEXRecorder:
    """Main class for recording u-blox data to RINEX"""
    
    def __init__(self, port, baudrate=38400, rinex_filename=None):
        self.port = port
        self.baudrate = baudrate
        self.serial_conn = None
        self.ubx = UBXMessage()
        
        if rinex_filename is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            rinex_filename = f"ublox_{timestamp}.24o"
            
        self.rinex_file = RINEXObservationFile(rinex_filename)
        self.current_gps_time = 0
        self.raw_measurements = defaultdict(dict)
        
    def connect(self):
        """Connect to u-blox receiver"""
        try:
            self.serial_conn = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=1.0,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                bytesize=serial.EIGHTBITS
            )
            print(f"Connected to {self.port} at {self.baudrate} baud")
            return True
        except Exception as e:
            print(f"Failed to connect: {e}")
            return False

    def parse_rawx_message(self, payload):
        """Parse UBX-RXM-RAWX message"""
        if len(payload) < 16:
            return None
            
        # Parse header
        rcv_tow, week, leap_s, num_meas, rec_stat, version, reserved = \
            struct.unpack('<dHbBBBH', payload[:16])
            
        measurements = []
        offset = 16
        
        for i in range(num_meas):
            if offset + 32 > len(payload):
                break

            cp_mes, pr_mes, do_mes, gnss_id, sv_id, sig_id, freq_id, locktime, cno, pr_stdev, cp_stdev, do_stdev, trk_stat, reserved2 = struct.unpack('<ddfBBHBBBBBBBB', payload[offset:offset+32])

            # Use gnss_id, sv_id, sig_id, freq_id as needed for satellite identification
            sat_id = sv_id  # You may want to use gnss_id and sv_id together for unique ID

            measurements.append({
                'sat_id': sat_id,
                'pseudorange': pr_mes,
                'carrier_phase': cp_mes,
                'doppler': do_mes,
                'cno': cno,
                'locktime': locktime,
                'freq_id': freq_id
            })

            offset += 32
            
        return {
            'rcv_tow': rcv_tow,
            'week': week,
            'measurements': measurements
        }
        
    def parse_timegps_message(self, payload):
        """Parse UBX-NAV-TIMEGPS message"""
        if len(payload) < 16:
            return None
            
        itow, ftow, week, leap_s, valid, t_acc = \
            struct.unpack('<LlHbBL', payload[:16])
            
        return {
            'itow': itow,
            'week': week,
            'valid': valid
        }
        
    def process_measurements(self, rawx_data):
        """Process raw measurements and prepare for RINEX output"""
        observations = {}
        satellites = []
        
        for meas in rawx_data['measurements']:
            sat_id = meas['sat_id']
            satellites.append(sat_id)
            
            # Convert measurements to RINEX observations
            obs = {}
            
            # Pseudorange observation (C1C)
            if meas['pseudorange'] != 0:
                obs['C1C'] = {
                    'value': meas['pseudorange'],
                    'lli': 0,
                    'strength': min(9, max(1, int(meas['cno'] / 6)))
                }
            
            # Carrier phase observation (L1C)
            if meas['carrier_phase'] != 0:
                # Convert cycles to distance (assuming L1 frequency)
                wavelength = 299792458.0 / 1575420000.0  # L1 wavelength
                obs['L1C'] = {
                    'value': meas['carrier_phase'] * wavelength,
                    'lli': 0,
                    'strength': min(9, max(1, int(meas['cno'] / 6)))
                }
            
            # Doppler observation (D1C)
            if meas['doppler'] != 0:
                obs['D1C'] = {
                    'value': meas['doppler'],
                    'lli': 0,
                    'strength': min(9, max(1, int(meas['cno'] / 6)))
                }
            
            # Signal strength (S1C)
            obs['S1C'] = {
                'value': meas['cno'],
                'lli': 0,
                'strength': min(9, max(1, int(meas['cno'] / 6)))
            }
            
            observations[sat_id] = obs
            
        return satellites, observations
        
    def read_and_process_data(self, duration_seconds):
        """Read data from receiver and process to RINEX"""
        self.rinex_file.open_file()
        print(f"Recording data for {duration_seconds} seconds...")
        
        start_time = time.time()
        buffer = b''
        
        try:
            while time.time() - start_time < duration_seconds:
                # Read data from serial port
                data = self.serial_conn.read(1024)
                if not data:
                    continue
                    
                buffer += data
                
                # print(repr(data)) # debug: print raw data
                
                # Process complete UBX messages
                while len(buffer) >= 8:
                    # Look for UBX sync bytes
                    sync_pos = buffer.find(b'\xB5\x62')
                    if sync_pos == -1:
                        buffer = buffer[-2:]  # Keep last 2 bytes in case of split sync
                        break
                        
                    if sync_pos > 0:
                        buffer = buffer[sync_pos:]
                        
                    if len(buffer) < 6:
                        break
                        
                    # Get message length
                    length = struct.unpack('<H', buffer[4:6])[0]
                    msg_len = 8 + length
                    
                    if len(buffer) < msg_len:
                        break
                        
                    # Parse complete message
                    msg_data = buffer[:msg_len]
                    buffer = buffer[msg_len:]
                    
                    msg = self.ubx.parse_message(msg_data)
                    if msg:
                        self._process_ubx_message(msg)
                        
        except KeyboardInterrupt:
            print("\nStopping data collection...")
        finally:
            self.rinex_file.close_file()
            print(f"Data saved to {self.rinex_file.filename}")
            
    def _process_ubx_message(self, msg):
        """Process parsed UBX message"""
        if (msg['class'], msg['id']) == self.ubx.UBX_RXM_RAWX:
            rawx_data = self.parse_rawx_message(msg['payload'])
            if rawx_data:
                # Calculate GPS time
                gps_time = rawx_data['week'] * 604800 + rawx_data['rcv_tow']
                
                # Process measurements
                satellites, observations = self.process_measurements(rawx_data)
                
                # Write to RINEX file
                if satellites:
                    self.rinex_file.write_observation_epoch(gps_time, satellites, observations)
                    print(f"Processed epoch with {len(satellites)} satellites")
                    
        elif (msg['class'], msg['id']) == self.ubx.UBX_NAV_TIMEGPS:
            timegps_data = self.parse_timegps_message(msg['payload'])
            if timegps_data:
                self.current_gps_time = timegps_data['week'] * 604800 + timegps_data['itow'] / 1000.0
                
    def disconnect(self):
        """Disconnect from receiver"""
        if self.serial_conn:
            self.serial_conn.close()
            print("Disconnected from receiver")

def main():
    parser = argparse.ArgumentParser(description='Record u-blox ZED-F9P data to RINEX format')
    parser.add_argument('--port', '-p', required=True, help='Serial port (e.g., /dev/ttyUSB0 or COM3)')
    parser.add_argument('--baudrate', '-b', type=int, default=38400, help='Baud rate (default: 38400)')
    parser.add_argument('--duration', '-d', type=int, default=3600, help='Recording duration in seconds (default: 3600)')
    parser.add_argument('--output', '-o', help='Output RINEX filename')
    
    args = parser.parse_args()
    
    # Create recorder instance
    recorder = UBloxRINEXRecorder(args.port, args.baudrate, args.output)
    
    # Connect
    if not recorder.connect():
        sys.exit(1)
        
    try:
        # Start recording
        recorder.read_and_process_data(args.duration)
        
    finally:
        recorder.disconnect()

if __name__ == "__main__":
    main()
