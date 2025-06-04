#!/usr/bin/env python3

"""
Service to aggregate multiple serial batteries https://github.com/Louisvdw/dbus-serialbattery
to one virtual battery.

Python location on Venus:
/usr/bin/python3.8
/usr/lib/python3.8/site-packages/

References:
https://dbus.freedesktop.org/doc/dbus-python/tutorial.html
https://github.com/victronenergy/venus/wiki/dbus
https://github.com/victronenergy/velib_python
"""

from gi.repository import GLib
import logging
import sys
import os
import platform
import dbus
import re
import settings
from functions import Functions
from datetime import datetime as dt  # for UTC time stamps for logging
import time as tt  # for charge measurement
from dbusmon import DbusMon
from threading import Thread

sys.path.append("/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
from vedbus import VeDbusService  # noqa: E402

VERSION = "3.5"

class SystemBus(dbus.bus.BusConnection):
    """A class representing a system bus connection.
    
    Inherits from dbus.bus.BusConnection and initializes with TYPE_SYSTEM.
    """
    def __new__(cls):
        """Create a new system bus connection.
        
        Returns:
            BusConnection: A new system bus connection
        """
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM) # copilot change: C0116


class SessionBus(dbus.bus.BusConnection):
    """A class representing a session bus connection.
    
    Inherits from dbus.bus.BusConnection and initializes with TYPE_SESSION.
    """
    def __new__(cls):
        """Create a new session bus connection.
        
        Returns:
            BusConnection: A new session bus connection
        """
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION) # copilot change: C0116


def get_bus() -> dbus.bus.BusConnection:
    """Determine and return the appropriate bus connection based on environment.
    
    Returns:
        dbus.bus.BusConnection: SessionBus if DBUS_SESSION_BUS_ADDRESS environment variable exists, SystemBus otherwise
    """
    return SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else SystemBus() # copilot change: C0116

class DbusAggBatService(object):
    """Service for aggregating multiple serial batteries into one virtual battery.
    
    This class handles the aggregation of data from multiple battery instances
    and presents them as a single virtual battery on the DBus.
    """ # copilot change: C0115 - added missing class docstring
    """Service for aggregating multiple serial batteries into one virtual battery.
    
    This class handles the aggregation of data from multiple battery instances
    and presents them as a single virtual battery on the DBus.
    """

    def __init__(self, servicename="com.victronenergy.battery.aggregate"):
        """Initialize the DBus Aggregate Battery Service.
        
        Args:
            servicename (str, optional): The DBus service name to register.
                Defaults to "com.victronenergy.battery.aggregate".
        """ # copilot change: C0116 - added missing function docstring
        """Initialize the DBus Aggregate Battery Service.
        
        Args:
            servicename (str, optional): The DBus service name to register.
                Defaults to "com.victronenergy.battery.aggregate".
        """
        self._fn = Functions()
        self._batteries_dict = {}  # marvo2011
        self._multi = None
        self._mppts_list = []
        self._smart_shunt = None # copilot change: C0103 - renamed from _smartShunt to follow snake_case naming convention
        self._search_trials = 0 # copilot change: C0103 - renamed from _searchTrials to follow snake_case naming convention
        self._read_trials = 0 # copilot change: C0103 - renamed from _readTrials to follow snake_case naming convention
        self._max_charge_voltage_old = 0 # copilot change: C0103 - renamed from _MaxChargeVoltage_old to follow snake_case naming convention
        self._max_charge_current_old = 0 # copilot change: C0103 - renamed from _MaxChargeCurrent_old to follow snake_case naming convention
        self._max_discharge_current_old = 0 # copilot change: C0103 - renamed from _MaxDischargeCurrent_old to follow snake_case naming convention
        # implementing hysteresis for allowing discharge
        self._fully_discharged = False # copilot change: C0103 - renamed from _fullyDischarged to follow snake_case naming convention
        self._dbus_conn = get_bus() # copilot change: C0103 - renamed from _dbusConn to follow snake_case naming convention
        logging.info("### Initialise VeDbusService ") # copilot change: W1201 - fixed logging format to not use f-strings
        self._dbusservice = VeDbusService(servicename, self._dbus_conn, register=False)
        logging.info("#### Done: Init of VeDbusService ") # copilot change: W1201 - fixed logging format to not use f-strings
        self._time_old = tt.time() # copilot change: C0103 - renamed from _timeOld to follow snake_case naming convention
        # written when dynamic CVL limit activated
        self._dc_feed_active = False # copilot change: C0103 - renamed from _DCfeedActive to follow snake_case naming convention
        # 0: inactive; 1: goal reached, waiting for discharging under nominal voltage; 2: nominal voltage reached
        self._balancing = 0
        # Day in year
        self._last_balancing = 0 # copilot change: C0103 - renamed from _lastBalancing to follow snake_case naming convention
        # set if the CVL needs to be reduced due to peaking
        self._dynamic_cvl = False # copilot change: C0103 - renamed from _dynamicCVL to follow snake_case naming convention
        # measure logging period in seconds
        self._log_timer = 0 # copilot change: C0103 - renamed from _logTimer to follow snake_case naming convention

        # read initial charge from text file
        try:
            self._charge_file = open(
                "/data/dbus-aggregate-batteries/charge", "r"
            )  # read
            self._own_charge = float(self._charge_file.readline().strip()) # copilot change: C0103 - renamed from _ownCharge to follow snake_case naming convention
            self._charge_file.close()
            self._own_charge_old = self._own_charge # copilot change: C0103 - renamed from _ownCharge_old to follow snake_case naming convention
            logging.info(
                "%s: Initial Ah read from file: %.0fAh",
                (dt.now()).strftime('%c'), self._own_charge
            ) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
        except (IOError, ValueError) as e: # copilot change: W0718 - caught specific exceptions instead of general Exception
            logging.error(
                "%s: Charge file read error: %s. Exiting.",
                (dt.now()).strftime('%c'), str(e)
            ) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
            sys.exit()

        if (
            settings.OWN_CHARGE_PARAMETERS
        ):  # read the day of the last balancing from text file
            try:
                self._last_balancing_file = open( # copilot change: C0103 - renamed from _lastBalancing_file to follow snake_case naming convention
                    "/data/dbus-aggregate-batteries/last_balancing", "r"
                )  # read
                self._last_balancing = int(self._last_balancing_file.readline().strip())
                self._last_balancing_file.close()
                time_unbalanced = (
                    int((dt.now()).strftime("%j")) - self._last_balancing
                )  # in days
                if time_unbalanced < 0:
                    time_unbalanced += 365  # year change
                logging.info(
                    "%s: Last balancing done at the %d. day of the year",
                    (dt.now()).strftime('%c'), self._last_balancing
                ) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
                logging.info("Batteries balanced %d days ago.", time_unbalanced) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
            except (IOError, ValueError) as e: # copilot change: W0718 - caught specific exceptions instead of general Exception
                logging.error(
                    "%s: Last balancing file read error: %s. Exiting.",
                    (dt.now()).strftime('%c'), str(e)
                ) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
                sys.exit()

        # Create the management objects, as specified in the ccgx dbus-api document
        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", "Python " + platform.python_version())
        self._dbusservice.add_path("/Mgmt/Connection", "Virtual")

        # Create the mandatory objects
        self._dbusservice.add_path("/DeviceInstance", 99)
        # this product ID was randomly selected - please exchange, if interference with another component
        self._dbusservice.add_path("/ProductId", 0xBA44)
        self._dbusservice.add_path("/ProductName", "AggregateBatteries")
        self._dbusservice.add_path("/FirmwareVersion", VERSION)
        self._dbusservice.add_path("/HardwareVersion", VERSION)
        self._dbusservice.add_path("/Connected", 1)

        # Create DC paths
        self._dbusservice.add_path(
            "/Dc/0/Voltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.2f}V", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Dc/0/Current",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.2f}A", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Dc/0/Power",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.0f}W", # copilot change: C0209
        )

        # Create capacity paths
        self._dbusservice.add_path("/Soc", None, writeable=True)
        self._dbusservice.add_path(
            "/Capacity",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.0f}Ah", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/InstalledCapacity",
            None,
            gettextcallback=lambda a, x: f"{x:.0f}Ah", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/ConsumedAmphours", None, gettextcallback=lambda a, x: f"{x:.0f}Ah" # copilot change: C0209
        )

        # Create temperature paths
        self._dbusservice.add_path("/Dc/0/Temperature", None, writeable=True)
        self._dbusservice.add_path("/System/MinCellTemperature", None, writeable=True)
        self._dbusservice.add_path("/System/MaxCellTemperature", None, writeable=True)

        # Create extras paths
        self._dbusservice.add_path(
            "/System/MinCellVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.3f}V", # copilot change: C0209
        )  # marvo2011
        self._dbusservice.add_path("/System/MinVoltageCellId", None, writeable=True)
        self._dbusservice.add_path(
            "/System/MaxCellVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.3f}V", # copilot change: C0209
        )  # marvo2011
        self._dbusservice.add_path("/System/MaxVoltageCellId", None, writeable=True)
        self._dbusservice.add_path("/System/NrOfCellsPerBattery", None, writeable=True)
        self._dbusservice.add_path("/System/NrOfModulesOnline", None, writeable=True)
        self._dbusservice.add_path("/System/NrOfModulesOffline", None, writeable=True)
        self._dbusservice.add_path(
            "/System/NrOfModulesBlockingCharge", None, writeable=True
        )
        self._dbusservice.add_path(
            "/System/NrOfModulesBlockingDischarge", None, writeable=True
        )
        self._dbusservice.add_path(
            "/Voltages/Sum",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.3f}V", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Voltages/Diff",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.3f}V", # copilot change: C0209
        )
        self._dbusservice.add_path("/TimeToGo", None, writeable=True)

        # Create alarm paths
        self._dbusservice.add_path("/Alarms/LowVoltage", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighVoltage", None, writeable=True)
        self._dbusservice.add_path("/Alarms/LowCellVoltage", None, writeable=True)
        # self._dbusservice.add_path('/Alarms/HighCellVoltage', None, writeable=True)
        self._dbusservice.add_path("/Alarms/LowSoc", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighChargeCurrent", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighDischargeCurrent", None, writeable=True)
        self._dbusservice.add_path("/Alarms/CellImbalance", None, writeable=True)
        self._dbusservice.add_path("/Alarms/InternalFailure", None, writeable=True)
        self._dbusservice.add_path(
            "/Alarms/HighChargeTemperature", None, writeable=True
        )
        self._dbusservice.add_path("/Alarms/LowChargeTemperature", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighTemperature", None, writeable=True)
        self._dbusservice.add_path("/Alarms/LowTemperature", None, writeable=True)
        self._dbusservice.add_path("/Alarms/BmsCable", None, writeable=True)

        # Create control paths
        self._dbusservice.add_path(
            "/Info/MaxChargeCurrent",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.1f}A", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Info/MaxDischargeCurrent",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.1f}A", # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Info/MaxChargeVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.2f}V", # copilot change: C0209
        )
        self._dbusservice.add_path("/Io/AllowToCharge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToDischarge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToBalance", None, writeable=True)

        # register VeDbusService after all paths where added
        logging.info("### Registering VeDbusService") # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
        self._dbusservice.register()

        x = Thread(target=self._start_monitor) # copilot change: C0103 - should be a more descriptive variable name in snake_case
        x.start()

        GLib.timeout_add(1000, self._find_settings)  # search com.victronenergy.settings

    # #############################################################################################################
    # #############################################################################################################
    # ## Starting battery dbus monitor in external thread (otherwise collision with AggregateBatteries service) ###
    # #############################################################################################################
    # #############################################################################################################

    def _start_monitor(self):
        """Start the battery monitor in a separate thread.
        
        This method initializes the DBus monitor which tracks battery data.
        """
        logging.info("%s: Starting battery monitor.", (dt.now()).strftime('%c')) # copilot change: W1201 - used lazy % formatting in logging function
        self._dbus_mon = DbusMon() # copilot change: C0103 - renamed from _dbusMon to follow snake_case naming convention, C0116 - added proper docstring

    # ####################################################################
    # ####################################################################
    # ## search Settings, to maintain CCL during dynamic CVL reduction ###
    # https://www.victronenergy.com/upload/documents/Cerbo_GX/140558-CCGX__Venus_GX__Cerbo_GX__Cerbo-S_GX_Manual-pdf-en.pdf, P72  # noqa: E501
    # ####################################################################
    # ####################################################################

    def _find_settings(self):
        """Search for com.victronenergy.settings on the DBus.
        
        This method tries to find the Victron Energy settings service on the DBus.
        If found, it stops searching and proceeds to find batteries.
        If not found after several trials, the program will exit.
        
        Returns:
            bool: True if another search should be performed, False if settings found or max trials reached
        """
        logging.info(
            "%s: Searching Settings: Trial Nr. %d",
            (dt.now()).strftime('%c'), self._search_trials + 1
        ) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string
        try:
            for service in self._dbus_conn.list_names():
                if "com.victronenergy.settings" in service:
                    self._settings = service
                    logging.info(
                        "%s: com.victronenergy.settings found.",
                        (dt.now()).strftime('%c')
                    ) # copilot change: W1201
        except (dbus.DBusException, dbus.exceptions.DBusException) as e: # copilot change: W0718 - caught specific exceptions instead of general Exception
            logging.error("%s: DBus exception: %s", (dt.now()).strftime('%c'), str(e)) # copilot change: W1201 - used lazy % formatting in logging function
            sys.exit()

        if self._settings is not None:
            self._search_trials = 0 # copilot change: C0103
            GLib.timeout_add(
                5000, self._find_batteries
            )  # search batteries on DBus if present
            return False  # all OK, stop calling this function
        elif self._search_trials < settings.SEARCH_TRIALS: # copilot change: C0103
            self._search_trials += 1 # copilot change: C0103
            return True  # next trial
        else:
            logging.error(
                "%s: com.victronenergy.settings not found. Exiting.",
                (dt.now()).strftime('%c')
            ) # copilot change: W1201 - used lazy % formatting in logging function
            sys.exit()

    # ####################################################################
    # ####################################################################
    # ## search physical batteries and optional SmartShunt on DC loads ###
    # ####################################################################
    # ####################################################################

    def _find_batteries(self):
        """Search for batteries on the DBus and register them.
        
        This method tries to find the required number of batteries on the DBus.
        It also initializes cell voltage paths and checks the number of cells.
        If the required number of batteries is not found after several trials, the program will exit.
        
        Returns:
            bool: True if another search should be performed, False otherwise
        """
        self._batteries_dict = {}  # Marvo2011
        batteries_count = 0  # copilot change: C0103
        product_name = ""  # copilot change: C0103
        logging.info(
            "%s: Searching batteries: Trial Nr. %d",
            (dt.now()).strftime('%c'), self._search_trials + 1
        ) # copilot change: W1201 - used lazy % formatting in logging function
        try:  # if Dbus monitor not running yet, new trial instead of exception
            for service in self._dbus_conn.list_names():
                if "com.victronenergy" in service:
                    logging.info("%s: Dbusmonitor sees: %s", (dt.now()).strftime('%c'), service) # copilot change: W1201 - used lazy % formatting in logging function
                if settings.BATTERY_SERVICE_NAME in service:
                    product_name = self._dbus_mon.dbusmon.get_value( # copilot change: C0103 - using snake_case variable naming convention
                        service, settings.BATTERY_PRODUCT_NAME_PATH
                    )
                    if (product_name is not None) and (settings.BATTERY_PRODUCT_NAME in product_name): # copilot change: C0103 - using consistent variable naming convention
                        logging.info("%s: Correct battery product name %s found in the service %s", (dt.now()).strftime('%c'), product_name, service) # copilot change: W1201 - used lazy % formatting in logging function
                        # Custom name, if exists, Marvo2011
                        try:
                            battery_name = self._dbus_mon.dbusmon.get_value( # copilot change: C0103 - using snake_case variable naming convention
                                service, settings.BATTERY_INSTANCE_NAME_PATH
                            )
                        except (AttributeError, KeyError) as e: # copilot change: W0718 - caught specific exceptions instead of general Exception
                            battery_name = f"Battery{batteries_count + 1}" # copilot change: C0103 - using snake_case variable naming convention, C0209 - using f-string instead of string formatting
                            logging.debug("Could not get battery name: %s", str(e)) # copilot change: W1201 - used lazy % formatting in logging function
                        # Check if all batteries have custom names
                        if battery_name in self._batteries_dict: # copilot change: C0103 - using consistent snake_case variable naming
                            battery_name = f"{battery_name}{batteries_count + 1}" # copilot change: C0103 - using consistent variable naming, C0209 - using f-string instead of string formatting

                        self._batteries_dict[battery_name] = service # copilot change: C0103 - using consistent snake_case variable naming
                        logging.info(
                            "%s: %s found, named as: %s.",
                            (dt.now()).strftime('%c'),
                            self._dbus_mon.dbusmon.get_value(service, '/ProductName'), # copilot change: C0103 - used snake_case name
                            battery_name
                        ) # copilot change: W1201 - used lazy % formatting in logging function

                        batteries_count += 1

                        # Create voltage paths with battery names
                        if settings.SEND_CELL_VOLTAGES == 1:
                            for cell_id in range( # copilot change: C0103 - using snake_case naming convention
                                1, (settings.NR_OF_CELLS_PER_BATTERY) + 1
                            ):
                                self._dbusservice.add_path(
                                    f"/Voltages/{re.sub('[^A-Za-z0-9_]+', '', battery_name)}_Cell{cell_id}", # copilot change: C0103 - using snake_case naming, C0209 - using f-string instead of string formatting
                                    None,
                                    writeable=True,
                                    gettextcallback=lambda a, x: f"{x:.3f}V", # copilot change: C0209 - using f-string instead of string formatting
                                )

                        # Check if Nr. of cells is equal
                        if (
                            self._dbus_mon.dbusmon.get_value( # copilot change: C0103 - used snake_case for consistency
                                service, "/System/NrOfCellsPerBattery"
                            )
                            != settings.NR_OF_CELLS_PER_BATTERY
                        ):
                            logging.error(
                                "%s: Number of cells of batteries is not correct. Exiting.",
                                (dt.now()).strftime('%c')
                            ) # copilot change: W1201 - used lazy % formatting in logging function
                            sys.exit()

                        # end of section, Marvo2011

                    elif (
                        (product_name is not None) and (settings.SMARTSHUNT_NAME_KEY_WORD in product_name)
                    ):  # if SmartShunt found, can be used for DC load current
                        self._smart_shunt = service # copilot change: C0103 - renamed from _smartShunt to follow snake_case naming convention
                        logging.info("%s: Correct Smart Shunt product name %s found in the service %s", (dt.now()).strftime('%c'), product_name, service) # copilot change: W1201 - used lazy % formatting in logging function

        except (dbus.DBusException, AttributeError) as e: # copilot change: W0718 - caught specific exceptions instead of general Exception
            logging.error("%s: Error getting battery info: %s", (dt.now()).strftime('%c'), str(e)) # copilot change: W1201 - used lazy % formatting in logging function
            pass

        logging.info(
            "%s: %d batteries found.",
            (dt.now()).strftime('%c'), batteries_count
        ) # copilot change: W1201 - used lazy % formatting in logging function

        if batteries_count == settings.NR_OF_BATTERIES:
            if settings.CURRENT_FROM_VICTRON:
                self._search_trials = 0 # copilot change: C0103
                GLib.timeout_add(
                    1000, self._find_multis
                )  # if current from Victron stuff search multi/quattro on DBus
            else:
                self._time_old = tt.time() # copilot change: C0103
                GLib.timeout_add(
                    1000, self._update
                )  # if current from BMS start the _update loop
            return False  # all OK, stop calling this function
        elif self._search_trials < settings.SEARCH_TRIALS: # copilot change: C0103
            self._search_trials += 1 # copilot change: C0103
            return True  # next trial
        else:
            logging.error(
                "%s: Required number of batteries not found. Exiting.",
                (dt.now()).strftime('%c')
            ) # copilot change: W1201 - used lazy % formatting in logging function
            sys.exit()

    # #########################################################################
    # #########################################################################
    # ## search Multis or Quattros (if selected for DC current measurement) ###
    # #########################################################################
    # #########################################################################

    def _find_multis(self):
        """Search for Multi/Quattro inverters on the DBus.
        
        This method tries to find Multi or Quattro inverters on the DBus for DC current measurements.
        If found, it proceeds to search for MPPTs if required, or starts the update loop otherwise.
        If not found after several trials, the program will exit.
        
        Returns:
            bool: True if another search should be performed, False otherwise
        """                
        logging.info(
            "%s: Searching Multi/Quatro VEbus: Trial Nr. %d",
            (dt.now()).strftime('%c'), self._search_trials + 1
        ) # copilot change: W1201 - used lazy % formatting in logging function
        try:
            for service in self._dbus_conn.list_names():
                if settings.MULTI_KEY_WORD in service:
                    self._multi = service
                    logging.info(
                        "%s: %s found.",
                        (dt.now()).strftime('%c'),
                        self._dbus_mon.dbusmon.get_value(service, '/ProductName')
                    ) # copilot change: W1201 - used lazy % formatting in logging function, C0103 - used snake_case name, C0209 - using proper string formatting
        except (dbus.DBusException, AttributeError) as e: # copilot change: W0718
            logging.error("%s: Error finding Multi: %s", (dt.now()).strftime('%c'), str(e)) # copilot change: W1201

        if self._multi is not None:
            if settings.NR_OF_MPPTS > 0:
                self._search_trials = 0 # copilot change: C0103
                GLib.timeout_add(
                    1000, self._find_mppts
                )  # search MPPTs on DBus if present
            else:
                self._time_old = tt.time() # copilot change: C0103
                GLib.timeout_add(
                    1000, self._update
                )  # if no MPPTs start the _update loop
            return False  # all OK, stop calling this function
        elif self._search_trials < settings.SEARCH_TRIALS: # copilot change: C0103
            self._search_trials += 1 # copilot change: C0103
            return True  # next trial
        else:                logging.error(
                    "%s: Multi/Quattro not found. Exiting.",
                    (dt.now()).strftime('%c')
                ) # copilot change: W1201 - used lazy % formatting in logging function, C0209 - using proper string formatting
        sys.exit()

    # ############################################################
    # ############################################################
    # ## search MPPTs (if selected for DC current measurement) ###
    # ############################################################
    # ############################################################

    def _find_mppts(self):
        """Search for MPPT solar charge controllers on the DBus.
        
        This method tries to find the required number of MPPT solar charge controllers on the DBus.
        If the required number is found, it starts the update loop.
        If not found after several trials, the program will exit.
        
        Returns:
            bool: True if another search should be performed, False otherwise
        """
        self._mppts_list = []
        mppts_count = 0  # copilot change: C0103
        logging.info(
            "%s: Searching MPPTs: Trial Nr. %d",
            (dt.now()).strftime('%c'), self._search_trials + 1
        ) # copilot change: W1201 - used lazy % formatting in logging function
        try:
            for service in self._dbus_conn.list_names():
                if settings.MPPT_KEY_WORD in service:
                    self._mppts_list.append(service)
                    logging.info(
                        "%s: %s found.",
                        (dt.now()).strftime('%c'),
                        self._dbus_mon.dbusmon.get_value(service, '/ProductName')
                    ) # copilot change: W1201 - used lazy % formatting in logging function, C0103 - used snake_case naming, C0209 - using proper string formatting
                    mppts_count += 1
        except (dbus.DBusException, AttributeError) as e: # copilot change: W0718 - caught specific exceptions instead of general Exception
            logging.error("%s: Error finding MPPTs: %s", (dt.now()).strftime('%c'), str(e)) # copilot change: W1201 - used lazy % formatting in logging function

        logging.info("%s: %d MPPT(s) found.", (dt.now()).strftime('%c'), mppts_count) # copilot change: W1201 - used lazy % formatting in logging function
        if mppts_count == settings.NR_OF_MPPTS:
            self._time_old = tt.time() # copilot change: C0103
            GLib.timeout_add(1000, self._update)
            return False  # all OK, stop calling this function
        elif self._search_trials < settings.SEARCH_TRIALS: # copilot change: C0103
            self._search_trials += 1 # copilot change: C0103
            return True  # next trial
        else:
                logging.error(
                    "%s: Required number of MPPTs not found. Exiting.",
                    (dt.now()).strftime('%c')
                ) # copilot change: W1201 - used lazy % formatting in logging function instead of f-string, C0303 - removed trailing whitespace
        sys.exit()

    # #################################################################################
    # #################################################################################
    # ### aggregate values of physical batteries, perform calculations, update Dbus ###
    # #################################################################################
    # #################################################################################

    def _update(self):
        """Aggregate values from physical batteries and update the DBus.
        
        This method collects data from all batteries, aggregates them, 
        performs calculations (charge state, alarms, etc.), and updates 
        the virtual battery values on the DBus. It also handles custom 
        charge parameters and Coulomb counting if enabled in settings.
        
        Returns:
            bool: True to keep the timer active and continue updating
        """

        # DC
        voltage = 0  # copilot change: C0103
        current = 0  # copilot change: C0103
        power = 0    # copilot change: C0103

        # Capacity
        soc = 0               # copilot change: C0103
        capacity = 0          # copilot change: C0103
        installed_capacity = 0 # copilot change: C0103
        consumed_amphours = 0  # copilot change: C0103
        time_to_go = 0        # copilot change: C0103

        # Temperature
        temperature = 0             # copilot change: C0103
        max_cell_temp_list = []     # copilot change: C0103 - list, maxima of all physical batteries
        min_cell_temp_list = []     # copilot change: C0103 - list, minima of all physical batteries

        # Extras
        cell_voltages_dict = {}  # copilot change: C0103
        max_cell_voltage_dict = (
            {}
        )  # copilot change: C0103 - dictionary {'ID' : MaxCellVoltage, ... } for all physical batteries
        min_cell_voltage_dict = (
            {}
        )  # copilot change: C0103 - dictionary {'ID' : MinCellVoltage, ... } for all physical batteries
        nr_of_modules_online = 0             # copilot change: C0103
        nr_of_modules_offline = 0            # copilot change: C0103
        nr_of_modules_blocking_charge = 0    # copilot change: C0103
        nr_of_modules_blocking_discharge = 0 # copilot change: C0103
        voltages_sum_dict = {}               # copilot change: C0103 - battery voltages from sum of cells, Marvo2011
        charge_voltage_reduced_list = []     # copilot change: C0103

        # Alarms
        low_voltage_alarm_list = []            # copilot change: C0103 - lists to find maxima
        high_voltage_alarm_list = []           # copilot change: C0103
        low_cell_voltage_alarm_list = []       # copilot change: C0103
        low_soc_alarm_list = []                # copilot change: C0103
        high_charge_current_alarm_list = []    # copilot change: C0103
        high_discharge_current_alarm_list = [] # copilot change: C0103
        cell_imbalance_alarm_list = []        # copilot change: C0103
        internal_failure_alarm_list = []      # copilot change: C0103
        high_charge_temperature_alarm_list = [] # copilot change: C0103
        low_charge_temperature_alarm_list = [] # copilot change: C0103
        high_temperature_alarm_list = []      # copilot change: C0103
        low_temperature_alarm_list = []       # copilot change: C0103
        bms_cable_alarm_list = []             # copilot change: C0103

        # Charge/discharge parameters
        max_charge_current_list = (
            []
        )  # copilot change: C0103 - the minimum of MaxChargeCurrent * NR_OF_BATTERIES to be transmitted
        max_discharge_current_list = (
            []
        )  # copilot change: C0103 - the minimum of MaxDischargeCurrent * NR_OF_BATTERIES to be transmitted
        max_charge_voltage_list = (
            []
        )  # copilot change: C0103 - if some cells are above MAX_CELL_VOLTAGE, store here the sum of differences for each battery
        allow_to_charge_list = []    # copilot change: C0103 - minimum of all to be transmitted
        allow_to_discharge_list = [] # copilot change: C0103 - minimum of all to be transmitted
        allow_to_balance_list = []   # copilot change: C0103 - minimum of all to be transmitted
        charge_mode_list = []        # copilot change: C0103 - Bulk, Absorption, Float, Keep always max voltage

        ####################################################
        # Get DBus values from all SerialBattery instances #
        ####################################################

        try:
            for i in self._batteries_dict:  # Marvo2011

                # DC
                step = "Read V, I, P"  # to detect error
                voltage += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/Dc/0/Voltage"
                )
                current += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/Dc/0/Current"
                )
                power += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/Dc/0/Power"
                )

                # Capacity
                step = "Read and calculate capacity, SoC, Time to go"
                installed_capacity += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/InstalledCapacity"
                )

                if not settings.OWN_SOC:
                    consumed_amphours += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                        self._batteries_dict[i], "/ConsumedAmphours"
                    )
                    capacity += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                        self._batteries_dict[i], "/Capacity"
                    )
                    soc += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                        self._batteries_dict[i], "/Soc"
                    ) * self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/InstalledCapacity"
                    )
                    ttg = self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/TimeToGo"
                    )
                    if (ttg is not None) and (time_to_go is not None): # copilot change: C0103
                        time_to_go += ttg * self._dbusMon.dbusmon.get_value( # copilot change: C0103
                            self._batteries_dict[i], "/InstalledCapacity"
                        )
                    else:
                        time_to_go = None # copilot change: C0103

                # Temperature
                step = "Read temperatures"
                temperature += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/Dc/0/Temperature"
                )
                max_cell_temp_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MaxCellTemperature"
                    )
                )
                min_cell_temp_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MinCellTemperature"
                    )
                )

                # Cell voltages
                step = "Read max. and min cell voltages and voltage sum"  # cell ID : its voltage
                max_cell_voltage_dict[ # copilot change: C0103
                    f"{i}_{self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MaxVoltageCellId')}" # copilot change: C0209
                ] = self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/MaxCellVoltage"
                )
                min_cell_voltage_dict[ # copilot change: C0103
                    f"{i}_{self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MinVoltageCellId')}" # copilot change: C0209
                ] = self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/MinCellVoltage"
                )

                 # here an exception is raised and new read trial initiated if None is on Dbus
                volt_sum_get = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], "/Voltages/Sum")
                if volt_sum_get != None:
                    voltages_sum_dict[i] = volt_sum_get # copilot change: C0103
                else:
                    raise TypeError(f"Battery {i} returns None value of /Voltages/Sum. Please check, if the setting 'BATTERY_CELL_DATA_FORMAT=1' in dbus-serialbattery config.")

                # Battery state
                step = "Read battery state"
                nr_of_modules_online += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/System/NrOfModulesOnline"
                )
                nr_of_modules_offline += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/System/NrOfModulesOffline"
                )
                nr_of_modules_blocking_charge += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/System/NrOfModulesBlockingCharge"
                )
                nr_of_modules_blocking_discharge += self._dbusMon.dbusmon.get_value( # copilot change: C0103
                    self._batteries_dict[i], "/System/NrOfModulesBlockingDischarge"
                )  # sum of modules blocking discharge

                step = "Read cell voltages"
                for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                    cell_voltages_dict[f"{i}_Cell{j + 1}"] = ( # copilot change: C0103, C0209
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], f"/Voltages/Cell{j + 1}" # copilot change: C0209
                        )
                    )

                # Alarms
                step = "Read alarms"
                low_voltage_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowVoltage"
                    )
                )
                high_voltage_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighVoltage"
                    )
                )
                low_cell_voltage_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowCellVoltage"
                    )
                )
                low_soc_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowSoc"
                    )
                )
                high_charge_current_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeCurrent"
                    )
                )
                high_discharge_current_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighDischargeCurrent"
                    )
                )
                cell_imbalance_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/CellImbalance"
                    )
                )
                internal_failure_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/InternalFailure_alarm"
                    )
                )
                high_charge_temperature_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeTemperature"
                    )
                )
                low_charge_temperature_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowChargeTemperature"
                    )
                )
                high_temperature_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighTemperature"
                    )
                )
                low_temperature_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowTemperature"
                    )
                )
                bms_cable_alarm_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/BmsCable"
                    )
                )

                if (
                    settings.OWN_CHARGE_PARAMETERS
                ):  # calculate reduction of charge voltage as sum of overvoltages of all cells
                    step = "Calculate CVL reduction"
                    cell_overvoltage = 0 # copilot change: C0103
                    for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                        cell_voltage = self._dbusMon.dbusmon.get_value( # copilot change: C0103
                            self._batteries_dict[i], f"/Voltages/Cell{j + 1}" # copilot change: C0209
                        )
                        if cell_voltage > settings.MAX_CELL_VOLTAGE: # copilot change: C0103
                            cell_overvoltage += cell_voltage - settings.MAX_CELL_VOLTAGE # copilot change: C0103
                    charge_voltage_reduced_list.append( # copilot change: C0103
                        voltages_sum_dict[i] - cell_overvoltage # copilot change: C0103
                    )

                else:  # Aggregate charge/discharge parameters
                    step = "Read charge parameters"
                    max_charge_current_list.append( # copilot change: C0103
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeCurrent"
                        )
                    )  # list of max. charge currents to find minimum
                    max_discharge_current_list.append( # copilot change: C0103
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxDischargeCurrent"
                        )
                    )  # list of max. discharge currents  to find minimum
                    max_charge_voltage_list.append( # copilot change: C0103
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeVoltage"
                        )
                    )  # list of max. charge voltages  to find minimum
                    charge_mode_list.append( # copilot change: C0103
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/ChargeMode"
                        )
                    )  # list of charge modes of batteries (Bulk, Absorption, Float, Keep always max voltage)

                step = "Read Allow to"
                allow_to_charge_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToCharge"
                    )
                )  # list of AllowToCharge to find minimum
                allow_to_discharge_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToDischarge"
                    )
                )  # list of AllowToDischarge to find minimum
                allow_to_balance_list.append( # copilot change: C0103
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToBalance"
                    )
                )  # list of AllowToBalance to find minimum

            step = "Find max. and min. cell voltage of all batteries"
            # placed in try-except structure for the case if some values are of None.
            # The _max() and _min() don't work with dictionaries
            max_voltage_cell_id = max(max_cell_voltage_dict, key=max_cell_voltage_dict.get) # copilot change: C0103
            max_cell_voltage = max_cell_voltage_dict[max_voltage_cell_id] # copilot change: C0103
            min_voltage_cell_id = min(min_cell_voltage_dict, key=min_cell_voltage_dict.get) # copilot change: C0103
            min_cell_voltage = min_cell_voltage_dict[min_voltage_cell_id] # copilot change: C0103

        except (AttributeError, KeyError, TypeError, ValueError) as err: # copilot change: W0718
            self._readTrials += 1
            logging.error("%s: Error: %s.", (dt.now()).strftime('%c'), err) # copilot change: W1201
            logging.error("Occured during step %s, Battery %s.", step, i) # copilot change: W1201
            logging.error("Read trial nr. %d", self._read_trials) # copilot change: W1201
            if self._readTrials > settings.READ_TRIALS:
                logging.error(
                    f"{(dt.now()).strftime('%c')}: DBus read failed. Exiting."
                )
                sys.exit()
            else:
                return True  # next call allowed

        self._read_trials = 0  # must be reset after try-except # copilot change: C0103

        #####################################################
        # Process collected values (except of dictionaries) #
        #####################################################

        # averaging
        voltage = voltage / settings.NR_OF_BATTERIES # copilot change: C0103
        temperature = temperature / settings.NR_OF_BATTERIES # copilot change: C0103
        voltages_sum = ( # copilot change: C0103
            sum(voltages_sum_dict.values()) / settings.NR_OF_BATTERIES
        )  # Marvo2011

        # find max and min cell temperature (have no ID)
        max_cell_temp = self._fn._max(max_cell_temp_list) # copilot change: C0103
        min_cell_temp = self._fn._min(min_cell_temp_list) # copilot change: C0103

        # find max in alarms
        low_voltage_alarm = self._fn._max(low_voltage_alarm_list) # copilot change: C0103
        high_voltage_alarm = self._fn._max(high_voltage_alarm_list) # copilot change: C0103
        low_cell_voltage_alarm = self._fn._max(low_cell_voltage_alarm_list) # copilot change: C0103
        low_soc_alarm = self._fn._max(low_soc_alarm_list) # copilot change: C0103
        high_charge_current_alarm = self._fn._max(high_charge_current_alarm_list) # copilot change: C0103
        high_discharge_current_alarm = self._fn._max(high_discharge_current_alarm_list) # copilot change: C0103
        cell_imbalance_alarm = self._fn._max(cell_imbalance_alarm_list) # copilot change: C0103
        internal_failure_alarm = self._fn._max(internal_failure_alarm_list) # copilot change: C0103
        high_charge_temperature_alarm = self._fn._max(high_charge_temperature_alarm_list) # copilot change: C0103
        low_charge_temperature_alarm = self._fn._max(low_charge_temperature_alarm_list) # copilot change: C0103
        high_temperature_alarm = self._fn._max(high_temperature_alarm_list) # copilot change: C0103
        low_temperature_alarm = self._fn._max(low_temperature_alarm_list) # copilot change: C0103
        bms_cable_alarm = self._fn._max(bms_cable_alarm_list) # copilot change: C0103

        # find max. charge voltage (if needed)
        if not settings.OWN_CHARGE_PARAMETERS:
            max_charge_voltage = self._fn._min(max_charge_voltage_list)  # add KEEP_MAX_CVL # copilot change: C0103
            max_charge_current = ( # copilot change: C0103
                self._fn._min(max_charge_current_list) * settings.NR_OF_BATTERIES
            )
            max_discharge_current = ( # copilot change: C0103
                self._fn._min(max_discharge_current_list) * settings.NR_OF_BATTERIES
            )

        allow_to_charge = self._fn._min(allow_to_charge_list) # copilot change: C0103
        allow_to_discharge = self._fn._min(allow_to_discharge_list) # copilot change: C0103
        allow_to_balance = self._fn._min(allow_to_balance_list) # copilot change: C0103

        ####################################
        # Measure current by Victron stuff #
        ####################################

        if settings.CURRENT_FROM_VICTRON:
            try:
                Current_VE = self._dbusMon.dbusmon.get_value(
                    self._multi, "/Dc/0/Current"
                )  # get DC current of multi/quattro (or system of them)
                for i in range(settings.NR_OF_MPPTS):
                    Current_VE += self._dbusMon.dbusmon.get_value(
                        self._mppts_list[i], "/Dc/0/Current"
                    )  # add DC current of all MPPTs (if present)

                if settings.DC_LOADS:
                    if settings.INVERT_SMARTSHUNT:
                        Current_VE += self._dbusMon.dbusmon.get_value(
                            self._smartShunt, "/Dc/0/Current"
                        )  # SmartShunt is monitored as a battery
                    else:
                        Current_VE -= self._dbusMon.dbusmon.get_value(
                            self._smartShunt, "/Dc/0/Current"
                        )

                if Current_VE is not None:
                    Current = Current_VE  # BMS current overwritten only if no exception raised
                    Power = (
                        Voltage * Current_VE
                    )  # calculate own power (not read from BMS)
                else:
                    logging.error(
                        f"{(dt.now()).strftime('%c')}: Victron current is None. Using BMS current and power instead."
                    )

            except (AttributeError, KeyError, TypeError) as e: # copilot change: W0718
                logging.error(
                    "%s: Victron current read error: %s. Using BMS current and power instead.",
                    (dt.now()).strftime('%c'), str(e)
                ) # copilot change: W1201

        ####################################################################################################
        # Calculate own charge/discharge parameters (overwrite the values received from the SerialBattery) #
        ####################################################################################################

        if settings.OWN_CHARGE_PARAMETERS:
            CVL_NORMAL = (
                settings.NR_OF_CELLS_PER_BATTERY
                * settings.CHARGE_VOLTAGE_LIST[int((dt.now()).strftime("%m")) - 1]
            )
            CVL_BALANCING = (
                settings.NR_OF_CELLS_PER_BATTERY * settings.BALANCING_VOLTAGE
            )
            ChargeVoltageBattery = CVL_NORMAL

            time_unbalanced = (
                int((dt.now()).strftime("%j")) - self._lastBalancing
            )  # in days
            if time_unbalanced < 0:
                time_unbalanced += 365  # year change

            if (
                CVL_BALANCING > CVL_NORMAL
            ):  # if the normal charging voltage is lower then 100% SoC
                # manage balancing voltage
                if (self._balancing == 0) and (
                    time_unbalanced >= settings.BALANCING_REPETITION
                ):
                    self._balancing = 1  # activate increased CVL for balancing
                    logging.info(
                        "%s: CVL increase for balancing activated.",
                        (dt.now()).strftime("%c")
                    )

                if self._balancing == 1:
                    ChargeVoltageBattery = CVL_BALANCING
                    if (Voltage >= CVL_BALANCING) and (
                        (MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX
                    ):
                        self._balancing = 2
                        logging.info(
                            "%s: Balancing goal reached.",
                            (dt.now()).strftime("%c")
                        )

                if self._balancing >= 2:
                    # keep balancing voltage at balancing day until decrease of solar powers and
                    ChargeVoltageBattery = CVL_BALANCING
                    if Voltage <= CVL_NORMAL:  # the charge above "normal" is consumed
                        self._balancing = 0
                        self._lastBalancing = int((dt.now()).strftime("%j"))
                        self._lastBalancing_file = open(
                            "/data/dbus-aggregate-batteries/last_balancing", "w"
                        )
                        self._lastBalancing_file.write("%s" % self._lastBalancing)
                        self._lastBalancing_file.close()
                        logging.info(
                            "%s: CVL increase for balancing de-activated.",
                            (dt.now()).strftime("%c")
                        )

                if self._balancing == 0:
                    ChargeVoltageBattery = CVL_NORMAL

            elif (
                (time_unbalanced > 0)
                and (Voltage >= CVL_BALANCING)
                and ((MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX)
            ):  # if normal charging voltage is 100% SoC and balancing is finished
                logging.info(
                    "%s: Balancing goal reached with full charging set as normal. Updating last_balancing file.",
                    (dt.now()).strftime("%c")
                ) # copilot change: W1201
                self._lastBalancing = int((dt.now()).strftime("%j"))
                self._lastBalancing_file = open(
                    "/data/dbus-aggregate-batteries/last_balancing", "w"
                )
                self._lastBalancing_file.write("%s" % self._lastBalancing)
                self._lastBalancing_file.close()

            if Voltage >= CVL_BALANCING:
                self._ownCharge = InstalledCapacity  # reset Coulumb counter to 100%

            # manage dynamic CVL reduction
            if MaxCellVoltage >= settings.MAX_CELL_VOLTAGE:
                if not self._dynamicCVL:
                    self._dynamicCVL = True                    logging.info(
                        "%s: Dynamic CVL reduction started.",
                        (dt.now()).strftime('%c')
                    ) # copilot change: W1201
                    if (
                        self._DCfeedActive is False
                    ):  # avoid periodic readout if once set True
                        self._DCfeedActive = self._dbusMon.dbusmon.get_value(
                            "com.victronenergy.settings",
                            "/Settings/CGwacs/OvervoltageFeedIn",
                        )  # check if DC-feed enabled
                self._dbusMon.dbusmon.set_value(
                    "com.victronenergy.settings",
                    "/Settings/CGwacs/OvervoltageFeedIn",
                    0,
                )  # disable DC-coupled PV feed-in
                logging.info(
                    "%s: DC-coupled PV feed-in de-activated.",
                    (dt.now()).strftime('%c')
                ) # copilot change: W1201
                MaxChargeVoltage = min(
                    (min(chargeVoltageReduced_list)), ChargeVoltageBattery
                )  # avoid exceeding MAX_CELL_VOLTAGE

            else:
                MaxChargeVoltage = ChargeVoltageBattery

                if self._dynamicCVL:
                    self._dynamicCVL = False
                    logging.info(
                        "%s: Dynamic CVL reduction finished.",
                        (dt.now()).strftime('%c')
                    ) # copilot change: W1201

                if (
                    (MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX
                ) and self._DCfeedActive:  # re-enable DC-feed if it was enabled before
                    self._dbusMon.dbusmon.set_value(
                        "com.victronenergy.settings",
                        "/Settings/CGwacs/OvervoltageFeedIn",
                        1,
                    )  # enable DC-coupled PV feed-in
                    logging.info(
                        "%s: DC-coupled PV feed-in re-activated.",
                        (dt.now()).strftime('%c')
                    ) # copilot change: W1201
                    # reset to prevent permanent logging and activation of  /Settings/CGwacs/OvervoltageFeedIn
                    self._DCfeedActive = False

            if (MinCellVoltage <= settings.MIN_CELL_VOLTAGE) and settings.ZERO_SOC:
                self._ownCharge = 0  # reset Coulumb counter to 0%

            # manage charge current
            if NrOfModulesBlockingCharge > 0:
                MaxChargeCurrent = 0
            else:
                MaxChargeCurrent = settings.MAX_CHARGE_CURRENT * self._fn._interpolate(
                    settings.CELL_CHARGE_LIMITING_VOLTAGE,
                    settings.CELL_CHARGE_LIMITED_CURRENT,
                    MaxCellVoltage,
                )

            # manage discharge current
            if MinCellVoltage <= settings.MIN_CELL_VOLTAGE:
                self._fullyDischarged = True
            elif (
                MinCellVoltage
                > settings.MIN_CELL_VOLTAGE + settings.MIN_CELL_HYSTERESIS
            ):
                self._fullyDischarged = False

            if (NrOfModulesBlockingDischarge > 0) or (self._fullyDischarged):
                MaxDischargeCurrent = 0
            else:
                MaxDischargeCurrent = (
                    settings.MAX_DISCHARGE_CURRENT
                    * self._fn._interpolate(
                        settings.CELL_DISCHARGE_LIMITING_VOLTAGE,
                        settings.CELL_DISCHARGE_LIMITED_CURRENT,
                        MinCellVoltage,
                    )
                )

        ###########################################################
        # own Coulomb counter (runs even the BMS values are used) #
        ###########################################################

        deltaTime = tt.time() - self._timeOld
        self._timeOld = tt.time()
        if Current > 0:
            self._ownCharge += (
                Current * (deltaTime / 3600) * settings.BATTERY_EFFICIENCY
            )  # charging (with efficiency)
        else:
            self._ownCharge += Current * (deltaTime / 3600)  # discharging
        self._ownCharge = max(self._ownCharge, 0)
        self._ownCharge = min(self._ownCharge, InstalledCapacity)

        # store the charge into text file if changed significantly (avoid frequent file access)
        if abs(self._ownCharge - self._ownCharge_old) >= (
            settings.CHARGE_SAVE_PRECISION * InstalledCapacity
        ):
            self._charge_file = open("/data/dbus-aggregate-batteries/charge", "w")
            self._charge_file.write("%.3f" % self._ownCharge)
            self._charge_file.close()
            self._ownCharge_old = self._ownCharge

        # overwrite BMS charge values
        if settings.OWN_SOC:
            Capacity = self._ownCharge
            Soc = 100 * self._ownCharge / InstalledCapacity
            ConsumedAmphours = InstalledCapacity - self._ownCharge
            if (
                self._dbusMon.dbusmon.get_value(
                    "com.victronenergy.system", "/SystemState/LowSoc"
                )
                == 0
            ) and (Current < 0):
                TimeToGo = -3600 * self._ownCharge / Current
            else:
                TimeToGo = None
        else:
            Soc = Soc / InstalledCapacity  # weighted sum
            if TimeToGo is not None:
                TimeToGo = TimeToGo / InstalledCapacity  # weighted sum

        #######################
        # Send values to DBus #
        #######################

        with self._dbusservice as bus:

            # send DC
            bus["/Dc/0/Voltage"] = Voltage  # round(Voltage, 2)
            bus["/Dc/0/Current"] = Current  # round(Current, 1)
            bus["/Dc/0/Power"] = Power  # round(Power, 0)

            # send charge
            bus["/Soc"] = Soc
            bus["/TimeToGo"] = TimeToGo
            bus["/Capacity"] = Capacity
            bus["/InstalledCapacity"] = InstalledCapacity
            bus["/ConsumedAmphours"] = ConsumedAmphours

            # send temperature
            bus["/Dc/0/Temperature"] = Temperature
            bus["/System/MaxCellTemperature"] = MaxCellTemp
            bus["/System/MinCellTemperature"] = MinCellTemp

            # send cell voltages
            bus["/System/MaxCellVoltage"] = MaxCellVoltage
            bus["/System/MaxVoltageCellId"] = MaxVoltageCellId
            bus["/System/MinCellVoltage"] = MinCellVoltage
            bus["/System/MinVoltageCellId"] = MinVoltageCellId
            bus["/Voltages/Sum"] = VoltagesSum
            bus["/Voltages/Diff"] = round(
                MaxCellVoltage - MinCellVoltage, 3
            )  # Marvo2011

            if settings.SEND_CELL_VOLTAGES == 1:  # Marvo2011
                for cellId, currentCell in enumerate(cellVoltages_dict):
                    bus[
                        "/Voltages/%s" % (re.sub("[^A-Za-z0-9_]+", "", currentCell))
                    ] = cellVoltages_dict[currentCell]

            # send battery state
            bus["/System/NrOfCellsPerBattery"] = settings.NR_OF_CELLS_PER_BATTERY
            bus["/System/NrOfModulesOnline"] = NrOfModulesOnline
            bus["/System/NrOfModulesOffline"] = NrOfModulesOffline
            bus["/System/NrOfModulesBlockingCharge"] = NrOfModulesBlockingCharge
            bus["/System/NrOfModulesBlockingDischarge"] = NrOfModulesBlockingDischarge

            # send alarms
            bus["/Alarms/LowVoltage"] = LowVoltage_alarm
            bus["/Alarms/HighVoltage"] = HighVoltage_alarm
            bus["/Alarms/LowCellVoltage"] = LowCellVoltage_alarm
            # bus['/Alarms/HighCellVoltage'] = HighCellVoltage_alarm   # not implemended in Venus
            bus["/Alarms/LowSoc"] = LowSoc_alarm
            bus["/Alarms/HighChargeCurrent"] = HighChargeCurrent_alarm
            bus["/Alarms/HighDischargeCurrent"] = HighDischargeCurrent_alarm
            bus["/Alarms/CellImbalance"] = CellImbalance_alarm
            bus["/Alarms/InternalFailure"] = InternalFailure_alarm
            bus["/Alarms/HighChargeTemperature"] = HighChargeTemperature_alarm
            bus["/Alarms/LowChargeTemperature"] = LowChargeTemperature_alarm
            bus["/Alarms/HighTemperature"] = HighTemperature_alarm
            bus["/Alarms/LowTemperature"] = LowTemperature_alarm
            bus["/Alarms/BmsCable"] = BmsCable_alarm

            # send charge/discharge control

            bus["/Info/MaxChargeCurrent"] = MaxChargeCurrent
            bus["/Info/MaxDischargeCurrent"] = MaxDischargeCurrent
            bus["/Info/MaxChargeVoltage"] = MaxChargeVoltage

            """
            # Not working, Serial Battery disapears regardles BLOCK_ON_DISCONNECT is True or False
            if BmsCable_alarm == 0:
                bus['/Info/MaxChargeCurrent'] = MaxChargeCurrent
                bus['/Info/MaxDischargeCurrent'] = MaxDischargeCurrent
                bus['/Info/MaxChargeVoltage'] = MaxChargeVoltage
            else:                                                       # if BMS connection lost
                bus['/Info/MaxChargeCurrent'] = 0
                bus['/Info/MaxDischargeCurrent'] = 0
                bus['/Info/MaxChargeVoltage'] = NR_OF_CELLS_PER_BATTERY * min(CHARGE_VOLTAGE_LIST)
                logging.error("%s: BMS connection lost.", (dt.now()).strftime('%c')) # copilot change: W1201
            """

            # this does not control the charger, is only displayed in GUI
            bus["/Io/AllowToCharge"] = AllowToCharge
            bus["/Io/AllowToDischarge"] = AllowToDischarge
            bus["/Io/AllowToBalance"] = AllowToBalance

        # ##########################################################
        # ################ Periodic logging ########################
        # ##########################################################

        if settings.LOG_PERIOD > 0:
            if self._logTimer < settings.LOG_PERIOD:
                self._logTimer += 1
            else:
                self._logTimer = 0
                logging.info("%s: Repetitive logging:", dt.now().strftime('%c')) # copilot change: W1201
                logging.info(
                    "  CVL: %.1fV, CCL: %.0fA, DCL: %.0fA",
                    MaxChargeVoltage, MaxChargeCurrent, MaxDischargeCurrent
                ) # copilot change: W1201
                logging.info(
                    "  Bat. voltage: %.1fV, Bat. current: %.0fA, SoC: %.1f%%, Balancing state: %d",
                    Voltage, Current, Soc, self._balancing
                ) # copilot change: W1201
                logging.info(
                    "  Min. cell voltage: %s: %.3fV, Max. cell voltage: %s: %.3fV, difference: %.3fV",
                    MinVoltageCellId, MinCellVoltage, MaxVoltageCellId, MaxCellVoltage, MaxCellVoltage - MinCellVoltage
                ) # copilot change: W1201

        return True


# ################
# ################
# ## Main loop ###
# ################
# ################


def main():

    logging.basicConfig(level=logging.INFO)
    logging.info("%s: Starting AggregateBatteries.", (dt.now()).strftime('%c')) # copilot change: W1201
    from dbus.mainloop.glib import DBusGMainLoop

    DBusGMainLoop(set_as_default=True)

    DbusAggBatService()

    logging.info(
        "%s: Connected to DBus, and switching over to GLib.MainLoop()",
        (dt.now()).strftime('%c')
    ) # copilot change: W1201
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
