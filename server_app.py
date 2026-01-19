from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.server.sync import ModbusTcpServer, ModbusConnectedRequestHandler
from pymodbus.framer.socket_framer import ModbusSocketFramer
import threading
import time
from datetime import datetime
from dotenv import load_dotenv
import os
import struct
import logging
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
import random
import socket

load_dotenv()

# Configure logging with rotation to avoid spam
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(LOG_DIR, 'server_app.log')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console handler (minimal)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # Only warnings+ to console
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# File handler with rotation (max 5MB, keep 3 backups)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# MongoDB configuration
MONGO_HOST = "vbserver"
MONGO_PORT = 27017

# Source of truth for devices:
#   DB: Meters
#   Collection: devices
MONGO_DB = "Meters"
MONGO_COLLECTION = "devices"
SERVER_PORT = 650
URL_PORT= 7000
# Global MongoDB client (Connection Pooling)
mongo_client = MongoClient(MONGO_HOST, MONGO_PORT, maxPoolSize=50, serverSelectionTimeoutMS=5000)

# Function to fetch configuration from MongoDB
def get_config():
    try:
        # Use global client
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]

        # Return both site and device name (keyed by server_unit_id)
        # For EM-groups, show the group name instead of individual device names
        Devices = {}
        em_groups_by_unit_id = {}  # Track EM groups by server_unit_id
        
        # First pass: collect EM groups
        for device in devices_collection.find({}, {"_id": 0, "server_unit_id": 1, "site": 1, "name": 1, "em_group": 1, "group_data_collection": 1}):
            if device.get("server_unit_id") is None:
                continue
            try:
                key = int(device["server_unit_id"])
            except Exception:
                continue
            
            em_group = device.get("em_group")
            if em_group:
                # This is an EM group member - track the group
                if key not in em_groups_by_unit_id:
                    em_groups_by_unit_id[key] = {
                        'em_group': em_group,
                        'group_data_collection': device.get('group_data_collection', ''),
                        'members': []
                    }
                em_groups_by_unit_id[key]['members'].append({
                    'site': device.get('site', ''),
                    'name': device.get('name', 'Unknown')
                })
        
        # Second pass: populate Devices dict
        # For EM-groups, use group name; for single devices, use device name
        for device in devices_collection.find({}, {"_id": 0, "server_unit_id": 1, "site": 1, "name": 1, "em_group": 1}):
            if device.get("server_unit_id") is None:
                continue
            try:
                key = int(device["server_unit_id"])
            except Exception:
                continue
            
            # If this server_unit_id has an EM group, use group info
            if key in em_groups_by_unit_id:
                group_info = em_groups_by_unit_id[key]
                Devices[key] = {
                    'site': group_info['em_group'],  # Use group name as site
                    'name': f"EM_GROUP: {group_info['em_group']}",  # Mark as EM group
                    'is_em_group': True,
                    'group_data_collection': group_info['group_data_collection'],
                    'member_count': len(group_info['members'])
                }
            else:
                # Single device (not part of EM group)
                if key not in Devices:  # Only set if not already set (avoid overwriting EM group)
                    Devices[key] = {
                        'site': device.get('site', ''),
                        'name': device.get('name', 'Unknown'),
                        'is_em_group': False
                    }
        
        #logger.info(f"Successfully fetched device list: {Devices}")
        return Devices
    except Exception as e:
        logger.error(f"Error fetching configuration from MongoDB: {e}")
        return {}

# Global device_list and lock for thread-safe updates
device_list = get_config()
device_list_lock = threading.Lock()

# Function to periodically update device_list
def update_device_list_periodically():
    global device_list
    while True:
        try:
            new_device_list = get_config()
            with device_list_lock:
                device_list = new_device_list
            # logger.info("Updated device_list from MongoDB")
        except Exception as e:
            logger.error(f"Error updating device_list: {e}")
        time.sleep(1*60)  # Sleep for 1 minute

# Start the periodic update thread
def start_device_list_updater():
    updater_thread = threading.Thread(target=update_device_list_periodically, daemon=True)
    updater_thread.start()
    logger.info("Started device_list update thread")

app = Flask(__name__)
app.secret_key = os.environ.get('SKEY') or 'your_strong_secret_key_here'

# Disable template caching for development
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Global variables
server_running = False
server_thread = None
modbus_context = None  # To store our modbus data
modbus_server = None  # To store server instance for proper shutdown
connected_clients = {}  # Dictionary {ip: connection time}

# Configuration for number of slaves and register size
NUM_SLAVES = 200
REGISTER_COUNT = 200000  # Must cover address 19026
SPECIAL_REGISTER_ADDRESS = 19026  # The specific register we want to track
SPECIAL_REGISTER_ADDRESS_1 = 19068  # The specific register we want to track
SPECIAL_REGISTER_ADDRESS_2 = 19076  # The specific register we want to track
# Simple authentication
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "password123"

def generate_random_data():
    data = {
        911: random.randint(1000000, 9999999),  # uint32: Meter Serial Num
        913: random.randint(100, 999),          # short: Firmware Version
        914: 0,                                 # Reserve
        915: 0,                                 # Reserve
        916: (random.randint(0, 1) << 8) | random.randint(0, 1),  # char: EI1 (high) | ED1 (low)
        917: (random.randint(0, 1) << 8),       # char: EC1 (high) | 0 (low)
        918: random.randint(100, 999),          # short: UMG 806 Hardware Version
        919: random.randint(100, 999),          # short: EI1 Hardware Version
        920: random.randint(100, 999),          # short: ED1 Hardware Version
        921: random.randint(100, 999)           # short: EC1 Hardware Version
    }
    return data

# Function to write data to the Modbus server
def write_serial_server():
    data = generate_random_data()
    registers = [
        data[911] & 0xFFFF,          # Low 16 bits of uint32 (911)
        (data[911] >> 16) & 0xFFFF,  # High 16 bits of uint32 (912)
        data[913],                   # Firmware Version (913)
        data[914],                   # Reserve (914)
        data[915],                   # Reserve (915)
        data[916],                   # EI1 | ED1 (916)
        data[917],                   # EC1 | 0 (917)
        data[918],                   # UMG 806 HW (918)
        data[919],                   # EI1 HW (919)
        data[920],                   # ED1 HW (920)
        data[921]                    # EC1 HW (921)
    ]

    return registers  # Only return the list of registers

# Custom request handler to track client connections
class CustomRequestHandler(ModbusConnectedRequestHandler):
    def setup(self):
        super().setup()
        ip = self.client_address[0]
        connected_clients[ip] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #logger.info(f"Client connected: {ip}")

    def finish(self):
        ip = self.client_address[0]  # Fixed: use client_address instead of clientaddress
        if ip in connected_clients:
            del connected_clients[ip]
        #logger.info(f"Client disconnected: {ip}")
        super().finish()

# Custom Modbus TCP server using our custom handler and proper framer
class CustomModbusTcpServer(ModbusTcpServer):
    allow_reuse_address = True  # Allow socket reuse to prevent "address already in use" errors

    def __init__(self, context, address):
        super().__init__(context, address=address, handler=CustomRequestHandler)

# Convert two 16-bit Modbus registers to a float32
def registers_to_float32(registers):
    try:
        if len(registers) < 2:
            #logger.error("Insufficient registers to convert to float32")
            return None
        high_word = registers[0]
        low_word = registers[1]
        packed = struct.pack('>HH', high_word, low_word)
        float_value = struct.unpack('>f', packed)[0]
        return float_value
    except Exception as e:
        #logger.error(f"Error converting registers to float32: {e}")
        return None

# Create a modbus context with multiple slaves
def create_modbus_context():
    slaves = {}
    try:
        for slave_id in range(1, NUM_SLAVES + 1):
            store = ModbusSlaveContext(
                di=ModbusSequentialDataBlock(0, [0] * REGISTER_COUNT),  # Discrete Inputs
                co=ModbusSequentialDataBlock(0, [0] * REGISTER_COUNT),  # Coils
                hr=ModbusSequentialDataBlock(0, [0] * REGISTER_COUNT),  # Holding Registers
                ir=ModbusSequentialDataBlock(0, [0] * REGISTER_COUNT)   # Input Registers
            )
            # Write the generated registers starting at address 911
            store.setValues(3, 911, write_serial_server())
            slaves[slave_id] = store
        return ModbusServerContext(slaves=slaves, single=False)
    except Exception as e:
        #logger.error(f"Error creating Modbus context: {e}")
        return None

# Run the modbus server on port 650
def run_modbus_server():
    global server_running, modbus_context, modbus_server
    modbus_context = create_modbus_context()

    if not modbus_context:
        logger.error("Failed to create Modbus context. Server cannot start.")
        return

    try:
        modbus_server = CustomModbusTcpServer(
            context=modbus_context,
            address=("0.0.0.0", SERVER_PORT)
        )
        # Socket reuse is now handled by the class attribute allow_reuse_address = True
        server_running = True
        logger.info("Modbus server started successfully on port 650")
        modbus_server.serve_forever()
    except OSError as e:
        if hasattr(e, 'winerror') and e.winerror == 10048:
            logger.error("Port 650 is already in use. Please stop any other Modbus server running on this port.")
        else:
            logger.error(f"Server error: {e}")
        server_running = False
        modbus_server = None
    except Exception as e:
        logger.error(f"Server error: {e}")
        server_running = False
        modbus_server = None

# Start the modbus server in a separate thread
def start_server_thread():
    global server_thread
    if not server_running:
        server_thread = threading.Thread(target=run_modbus_server, daemon=True)
        server_thread.start()
        time.sleep(1)  # Give the server a moment to start

# Simulated stop of the server
def stop_server():
    global server_running, server_thread, modbus_context, modbus_server
    if server_running:
        logger.info("Stopping Modbus server...")
        server_running = False

        # Shutdown the server properly
        if modbus_server:
            try:
                modbus_server.server_close()
                modbus_server.shutdown()
                logger.info("Server shutdown command sent")
            except Exception as e:
                logger.error(f"Error during server shutdown: {e}")

        modbus_context = None
        modbus_server = None

        # Wait for thread to finish
        if server_thread:
            server_thread.join(timeout=3)
        server_thread = None
        logger.info("Modbus server stopped")

# Safely get the float32 value from specific registers
def get_float32_value(slave_context, register_address):
    try:
        values = slave_context.getValues(3, register_address, 2)
        if values and len(values) == 2:
            float_value = registers_to_float32(values)
            return float_value
        return None
    except Exception as e:
        logger.error(f"Error reading float32 at register {register_address}: {e}")
        return None

# Main route for monitoring – shows connected clients and slave values
@app.route('/')
def index():
    slave_values = {}
    if modbus_context:
        for slave_id in range(1, NUM_SLAVES + 1):
            try:
                slave_ctx = modbus_context[slave_id]
                values = []
                values.append(get_float32_value(slave_ctx, SPECIAL_REGISTER_ADDRESS))
                values.append(get_float32_value(slave_ctx, SPECIAL_REGISTER_ADDRESS_1))
                values.append(get_float32_value(slave_ctx, SPECIAL_REGISTER_ADDRESS_2))
                if all(v is not None for v in values):
                    slave_values[slave_id] = [f"{v:.2f}" for v in values]
                else:
                    slave_values[slave_id] = ["N/A"] * len(values)
            except Exception as ex:
                logger.error(f"Error processing slave {slave_id}: {ex}")
                slave_values[slave_id] = ["Error"] * 3

    with device_list_lock:
        current_device_list = device_list

    return render_template("livedat.html",
                           server_status=server_running,
                           clients=connected_clients,
                           slave_values=slave_values,
                           sites=current_device_list)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

SERVER_PASSWORD = os.getenv('SERVER_PASSWORD')  # Get password from .env

@app.route('/start_server', methods=['POST'])
def start_server_route():
    password = request.form.get('password')
    if password != "farhad":
        return "Unauthorized: Incorrect password", 403  # Return error if password is incorrect

    start_server_thread()
    return redirect(url_for('index'))  # Redirect back to monitoring page after starting

@app.route('/stop_server', methods=['POST'])
def stop_server_route():
    password = request.form.get('password')
    if password != SERVER_PASSWORD:
        return "Unauthorized: Incorrect password", 403  # Return error if password is incorrect

    stop_server()
    return redirect(url_for('index'))

@app.route('/clients')
def clients():
    """Display all clients in a table for management"""
    try:
        # Use global client
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]
        all_devices = list(devices_collection.find({}, {"_id": 0}))
        
        # Group EM devices by em_group and server_unit_id
        em_groups = {}  # {(em_group, server_unit_id): [devices]}
        single_devices = []
        
        for device in all_devices:
            em_group = device.get('em_group')
            if em_group:
                key = (em_group, device.get('server_unit_id'))
                if key not in em_groups:
                    em_groups[key] = []
                em_groups[key].append(device)
            else:
                single_devices.append(device)
        
        # Convert EM groups to display format (one row per group)
        grouped_devices = []
        for (em_group, server_unit_id), members in em_groups.items():
            if members:
                first = members[0]
                grouped_devices.append({
                    'is_em_group': True,
                    'em_group': em_group,
                    'server_unit_id': server_unit_id,
                    'project_nr': first.get('project_nr'),
                    'site': em_group,
                    'name': f"EM_GROUP: {em_group}",
                    'group_data_collection': first.get('group_data_collection'),
                    'members': members,
                    'member_count': len(members)
                })
        
        # Combine single devices and grouped EM devices
        devices = single_devices + grouped_devices
        return render_template('clients.html', devices=devices)
    except Exception as e:
        logger.error(f"Error loading clients page: {e}")
        return render_template('clients.html', devices=[])

@app.route('/add_device', methods=['POST'])
def add_device():
    """Add a new device to the MongoDB configuration"""
    try:
        # Get JSON data from request
        device_data = request.get_json()
        
        # Debug: log what we received
        logger.info(f"ADD_DEVICE received: em_group='{device_data.get('em_group')}', group_data_collection='{device_data.get('group_data_collection')}', data_collection='{device_data.get('data_collection')}'")

        # Normalize/validate read type (holding/input). Default is holding.
        reading = str(device_data.get('reading') or device_data.get('table') or 'holding').strip().lower()
        if reading not in ('holding', 'input'):
            return jsonify({'success': False, 'error': 'Invalid reading type. Must be "holding" or "input"'}), 400
        device_data['reading'] = reading

        # For em_group devices, use group_data_collection instead of data_collection.
        # Individual data_collection is not required for em_group members.
        # Check if em_group has an actual non-empty value (not just empty string)
        has_em_group = bool(device_data.get('em_group') and str(device_data.get('em_group')).strip())
        logger.info(f"ADD_DEVICE: has_em_group={has_em_group}")
        
        if has_em_group:
            required_fields = ['site', 'name', 'ip', 'port', 'unit_id', 'server_unit_id', 'registers']
        else:
            required_fields = ['site', 'name', 'ip', 'port', 'unit_id', 'server_unit_id', 'data_collection', 'registers']

        for field in required_fields:
            # Check both if field exists AND if it has a non-empty value
            value = device_data.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400

        # Optional fields - set defaults if not provided
        if 'project_nr' not in device_data:
            device_data['project_nr'] = None
        if 'em_group' not in device_data:
            device_data['em_group'] = None
        
        # Use group_data_collection PRECISELY as provided, or auto-generate if empty
        if has_em_group:
            group_data_collection = device_data.get('group_data_collection', '').strip()
            if not group_data_collection or group_data_collection.lower() == 'undefined':
                # Auto-generate as project_nr_Device_Name only if completely empty
                project_nr = str(device_data.get('project_nr', '')).strip()
                device_name = str(device_data.get('em_group', '')).strip()
                if not device_name:
                    device_name = str(device_data.get('name', '')).strip()
                
                # Clean device name: remove special characters, keep only alphanumeric and underscore
                device_name_clean = "".join(c if c.isalnum() or c == "_" else "_" for c in device_name.replace(" ", "_"))
                
                if project_nr:
                    device_data['group_data_collection'] = f"{project_nr}_{device_name_clean}"
                else:
                    device_data['group_data_collection'] = device_name_clean
                logger.info(f"Auto-generated group_data_collection: {device_data['group_data_collection']}")
            else:
                # Use the EXACT value provided by user - no modifications
                device_data['group_data_collection'] = group_data_collection
                logger.info(f"Using provided group_data_collection: {group_data_collection}")

        # Validate server_unit_id range (1-200, matching NUM_SLAVES)
        try:
            server_unit_id = int(device_data.get('server_unit_id'))
            if server_unit_id < 1 or server_unit_id > NUM_SLAVES:
                return jsonify({'success': False, 'error': f'Server Unit ID must be between 1 and {NUM_SLAVES}'}), 400
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Invalid server_unit_id format. Must be a number'}), 400

        # Connect to MongoDB using global client
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]

        # Check if server_unit_id already exists (unless it's part of an em_group)
        existing_devices = list(devices_collection.find(
            {"server_unit_id": server_unit_id}, 
            {"_id": 0, "em_group": 1, "site": 1, "name": 1}
        ))
        
        if existing_devices:
            # If not part of EM group, check if any existing device is also not in EM group
            existing_non_group = [d for d in existing_devices if not d.get('em_group')]
            if existing_non_group and not has_em_group:
                existing_site = existing_non_group[0].get('site', 'Unknown')
                existing_name = existing_non_group[0].get('name', 'Unknown')
                return jsonify({
                    'success': False, 
                    'error': f'Server Unit ID {server_unit_id} is already in use by "{existing_site}" ({existing_name}). Use em_group if multiple devices should write to same unit_id'
                }), 400
            
            # If adding to EM group, check if existing devices are in different EM group
            if has_em_group:
                different_groups = [d for d in existing_devices if d.get('em_group') and d.get('em_group') != device_data.get('em_group')]
                if different_groups:
                    existing_group = different_groups[0].get('em_group', 'Unknown')
                    return jsonify({
                        'success': False,
                        'error': f'Server Unit ID {server_unit_id} is already used by EM group "{existing_group}". Cannot use same ID for different EM group "{device_data.get("em_group")}"'
                    }), 400

        devices_collection.insert_one(device_data)
        logger.info(f"Successfully added new device: {device_data.get('site')}")

        # Update the device list immediately
        global device_list
        new_device_list = get_config()
        with device_list_lock:
            device_list = new_device_list

        return jsonify({'success': True, 'message': 'Device added successfully'}), 200

    except Exception as e:
        logger.error(f"Error adding device: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/update_device', methods=['POST'])
def update_device():
    """Update an existing device in the MongoDB configuration"""
    try:
        device_data = request.get_json()
        original_server_unit_id = device_data.get('original_server_unit_id')

        if not original_server_unit_id:
            return jsonify({'success': False, 'error': 'Missing original server_unit_id'}), 400

        # Convert to int for consistent comparison
        try:
            original_server_unit_id = int(original_server_unit_id)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Invalid original_server_unit_id format'}), 400

        # Remove the original_server_unit_id from device_data before updating
        device_data.pop('original_server_unit_id', None)

        # Normalize/validate read type (holding/input). Default is holding.
        reading = str(device_data.get('reading') or device_data.get('table') or 'holding').strip().lower()
        if reading not in ('holding', 'input'):
            return jsonify({'success': False, 'error': 'Invalid reading type. Must be "holding" or "input"'}), 400
        device_data['reading'] = reading

        # Check if this is an EM group device
        has_em_group = bool(device_data.get('em_group') and str(device_data.get('em_group')).strip())
        
        # Use group_data_collection PRECISELY as provided, or auto-generate if empty
        if has_em_group:
            group_data_collection = device_data.get('group_data_collection', '').strip()
            if not group_data_collection or group_data_collection.lower() == 'undefined':
                # Auto-generate as project_nr_Device_Name only if completely empty
                project_nr = str(device_data.get('project_nr', '')).strip()
                device_name = str(device_data.get('em_group', '')).strip()
                if not device_name:
                    device_name = str(device_data.get('name', '')).strip()
                
                # Clean device name: remove special characters, keep only alphanumeric and underscore
                device_name_clean = "".join(c if c.isalnum() or c == "_" else "_" for c in device_name.replace(" ", "_"))
                
                if project_nr:
                    device_data['group_data_collection'] = f"{project_nr}_{device_name_clean}"
                else:
                    device_data['group_data_collection'] = device_name_clean
                logger.info(f"Auto-generated group_data_collection in update: {device_data['group_data_collection']}")
            else:
                # Use the EXACT value provided by user - no modifications
                device_data['group_data_collection'] = group_data_collection
                logger.info(f"Using provided group_data_collection in update: {group_data_collection}")

        # Validate server_unit_id range (1-200, matching NUM_SLAVES)
        try:
            new_server_unit_id = int(device_data.get('server_unit_id'))
            if new_server_unit_id < 1 or new_server_unit_id > NUM_SLAVES:
                return jsonify({'success': False, 'error': f'Server Unit ID must be between 1 and {NUM_SLAVES}'}), 400
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Invalid server_unit_id format. Must be a number'}), 400

        # Connect to MongoDB using global client
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]

        # Find the device being updated
        existing = devices_collection.find_one({"server_unit_id": original_server_unit_id})
        if not existing:
            return jsonify({'success': False, 'error': 'Device not found'}), 404

        # Check if new server_unit_id conflicts with another device
        if new_server_unit_id != original_server_unit_id:
            existing_em_group = existing.get('em_group')
            new_em_group = device_data.get('em_group')
            
            # Find all devices using the new server_unit_id
            conflicting_devices = list(devices_collection.find(
                {"server_unit_id": new_server_unit_id},
                {"_id": 1, "em_group": 1, "site": 1, "name": 1}
            ))
            
            # Filter out the device we're updating (if it exists in the list)
            conflicting_devices = [d for d in conflicting_devices if d['_id'] != existing['_id']]
            
            if conflicting_devices:
                # Check if any conflict is with non-EM-group device
                non_group_conflicts = [d for d in conflicting_devices if not d.get('em_group')]
                if non_group_conflicts and not new_em_group:
                    conflict_site = non_group_conflicts[0].get('site', 'Unknown')
                    conflict_name = non_group_conflicts[0].get('name', 'Unknown')
                    return jsonify({
                        'success': False,
                        'error': f'Server Unit ID {new_server_unit_id} is already in use by "{conflict_site}" ({conflict_name})'
                    }), 400
                
                # Check if trying to use ID from different EM group
                different_groups = [d for d in conflicting_devices if d.get('em_group') and d.get('em_group') != new_em_group]
                if different_groups:
                    conflict_group = different_groups[0].get('em_group', 'Unknown')
                    return jsonify({
                        'success': False,
                        'error': f'Server Unit ID {new_server_unit_id} is already used by EM group "{conflict_group}". Cannot use same ID for different EM group'
                    }), 400

        device_data['_id'] = existing['_id']
        res = devices_collection.replace_one({"_id": existing["_id"]}, device_data)

        if res.modified_count > 0:
            logger.info(f"Successfully updated device: {device_data.get('site')}")
        else:
            logger.info(f"Device update no-op: {device_data.get('site')}")

        # Update the device list immediately
        global device_list
        new_device_list = get_config()
        with device_list_lock:
            device_list = new_device_list
        return jsonify({'success': True, 'message': 'Device updated successfully'}), 200

    except Exception as e:
        logger.error(f"Error updating device: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/delete_device', methods=['POST'])
def delete_device():
    """Delete a device from the MongoDB configuration"""
    try:
        data = request.get_json()
        server_unit_id = data.get('server_unit_id')

        if not server_unit_id:
            return jsonify({'success': False, 'error': 'Missing server_unit_id'}), 400

        # Connect to MongoDB using global client
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]

        result = devices_collection.delete_one({"server_unit_id": server_unit_id})

        if result.deleted_count > 0:
            logger.info(f"Successfully deleted device with server_unit_id: {server_unit_id}")
            # Update the device list immediately
            global device_list
            new_device_list = get_config()
            with device_list_lock:
                device_list = new_device_list
            return jsonify({'success': True, 'message': 'Device deleted successfully'}), 200
        return jsonify({'success': False, 'error': 'Device not found'}), 404

    except Exception as e:
        logger.error(f"Error deleting device: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/get_all_devices', methods=['GET'])
def get_all_devices():
    """Get all devices from MongoDB configuration"""
    try:
        # Use global client
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]

        devices = list(devices_collection.find({}, {"_id": 0, "server_unit_id": 1, "em_group": 1, "site": 1}))
        return jsonify({'success': True, 'devices': devices}), 200

    except Exception as e:
        logger.error(f"Error fetching devices: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint with system status and recent errors"""
    try:
        # Check MongoDB connection
        mongo_status = "OK"
        try:
            mongo_client.admin.command('ping')
        except Exception as e:
            mongo_status = f"ERROR: {e}"

        # Get device count
        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]
        device_count = devices_collection.count_documents({})

        # Get recent errors from service_events in Meters database
        recent_errors = []
        try:
            meters_db = mongo_client["Meters"]
            events_coll = meters_db["service_events"]
            # Get last 50 errors, sorted by last_seen descending
            cursor = events_coll.find(
                {"severity": {"$in": ["error", "warning"]}},
                {"_id": 0, "site": 1, "category": 1, "message": 1, "severity": 1, "count": 1, "last_seen": 1}
            ).sort("last_seen", -1).limit(50)
            recent_errors = list(cursor)
            # Format datetime for display
            for err in recent_errors:
                if "last_seen" in err and err["last_seen"]:
                    err["last_seen"] = err["last_seen"].strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass  # service_events might not exist yet

        return render_template('health.html',
                               mongo_status=mongo_status,
                               modbus_status="Kører" if server_running else "Stoppet",
                               device_count=device_count,
                               connected_clients=len(connected_clients),
                               recent_errors=recent_errors)

    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health/json')
def health_json():
    """Health check endpoint (JSON format for monitoring tools)"""
    try:
        mongo_ok = True
        try:
            mongo_client.admin.command('ping')
        except Exception:
            mongo_ok = False

        db = mongo_client[MONGO_DB]
        devices_collection = db[MONGO_COLLECTION]
        device_count = devices_collection.count_documents({})

        return jsonify({
            'status': 'healthy' if mongo_ok else 'degraded',
            'mongodb': 'connected' if mongo_ok else 'disconnected',
            'modbus_server': 'running' if server_running else 'stopped',
            'device_count': device_count,
            'connected_clients': len(connected_clients),
            'timestamp': datetime.now().isoformat()
        }), 200

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    # Start the device_list updater thread
    start_device_list_updater()
    # Start Flask server on port 7000
    # start_server_thread()  # Removed auto-start of Modbus TCP server
    # Enable debug mode and disable reloader to prevent template caching
    app.run(host='0.0.0.0', port=URL_PORT, debug=True, use_reloader=False)