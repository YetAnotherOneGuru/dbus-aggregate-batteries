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

# copilot change: Reorganized imports to comply with PEP8 - standard imports first
import logging
import os
import platform
import re
import sys
from datetime import datetime as dt  # for UTC time stamps for logging
from threading import Thread
import time as tt  # for charge measurement

# copilot change: Third-party imports after standard library imports
from gi.repository import GLib
import dbus

# copilot change: First-party imports after third-party imports
import settings
from functions import Functions
from dbusmon import DbusMon

# copilot change: Venus OS specific imports last
sys.path.append("/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
from vedbus import VeDbusService  # noqa: E402

VERSION = "3.5"

# copilot change: Added class docstring
class SystemBus(dbus.bus.BusConnection):
    """SystemBus connection class for DBus"""
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


# copilot change: Added class docstring
class SessionBus(dbus.bus.BusConnection):
    """SessionBus connection class for DBus"""
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)


# copilot change: Added function docstring
def get_bus() -> dbus.bus.BusConnection:
    """Return the appropriate bus based on environment"""
    return SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else SystemBus()

# copilot change: Added class docstring
class DbusAggBatService:
    """
    Service to aggregate battery data from multiple serial batteries 
    and present them as a single battery on the dbus
    """

    def __init__(self, servicename="com.victronenergy.battery.aggregate"):
        self._fn = Functions()
        self._batteries_dict = {}  # marvo2011
        self._multi = None
        self._mppts_list = []
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._smart_shunt = None  # was: _smartShunt
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._search_trials = 0  # was: _searchTrials
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._read_trials = 0  # was: _readTrials
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._max_charge_voltage_old = 0  # was: _MaxChargeVoltage_old
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._max_charge_current_old = 0  # was: _MaxChargeCurrent_old
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._max_discharge_current_old = 0  # was: _MaxDischargeCurrent_old
        # implementing hysteresis for allowing discharge
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._fully_discharged = False  # was: _fullyDischarged
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._dbus_conn = get_bus()  # was: _dbusConn
        logging.info("### Initialise VeDbusService ")
        self._dbusservice = VeDbusService(servicename, self._dbus_conn, register=False)
        logging.info("#### Done: Init of VeDbusService ")
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._time_old = tt.time()  # was: _timeOld
        # written when dynamic CVL limit activated
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._dc_feed_active = False  # was: _DCfeedActive
        # 0: inactive; 1: goal reached, waiting for discharging under nominal voltage; 2: nominal voltage reached
        self._balancing = 0
        # Day in year
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._last_balancing = 0  # was: _lastBalancing
        # set if the CVL needs to be reduced due to peaking
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._dynamic_cvl = False  # was: _dynamicCVL
        # measure logging period in seconds
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._log_timer = 0  # was: _logTimer

        # read initial charge from text file
        try:
            # copilot change: Using 'with' for resource-allocating operations and specifying encoding
            with open(
                "/data/dbus-aggregate-batteries/charge", "r", encoding="utf-8"
            ) as self._charge_file:  # read
                # copilot change: renamed variable to follow snake_case convention
                self._own_charge = float(self._charge_file.readline().strip())
            # copilot change: renamed variable to follow snake_case convention
            self._own_charge_old = self._own_charge
            # copilot change: Using lazy formatting in logging with f-strings
            logging.info(
                f"{dt.now().strftime('%c')}: Initial Ah read from file: {self._own_charge:.0f}Ah"
            )
        # copilot change: Catching specific exceptions instead of general Exception
        except (IOError, ValueError) as err:
            # copilot change: Using lazy formatting in logging with f-strings
            logging.error(
                f"{dt.now().strftime('%c')}: Charge file read error: {err}. Exiting."
            )
            sys.exit()

        if (
            settings.OWN_CHARGE_PARAMETERS
        ):  # read the day of the last balancing from text file
            try:
                # copilot change: Using 'with' for resource-allocating operations and specifying encoding
                # copilot change: renamed variable to follow snake_case convention
                with open(
                    "/data/dbus-aggregate-batteries/last_balancing", "r", encoding="utf-8"
                ) as self._last_balancing_file:  # read
                    # copilot change: renamed variable to follow snake_case convention
                    self._last_balancing = int(self._last_balancing_file.readline().strip())
                # copilot change: calculating days since last balancing
                time_unbalanced = (
                    int(dt.now().strftime("%j")) - self._last_balancing
                )  # in days
                if time_unbalanced < 0:
                    time_unbalanced += 365  # year change
                # copilot change: Using lazy formatting in logging with f-strings
                logging.info(
                    f"{dt.now().strftime('%c')}: Last balancing done at the {self._last_balancing}. day of the year"
                )
                # copilot change: Using f-string
                logging.info(f"Batteries balanced {time_unbalanced} days ago.")
            # copilot change: Catching specific exceptions instead of general Exception
            except (IOError, ValueError) as err:
                # copilot change: Using lazy formatting in logging with f-strings
                logging.error(
                    f"{dt.now().strftime('%c')}: Last balancing file read error: {err}. Exiting."
                )
                sys.exit()

        # Create the management objects, as specified in the ccgx dbus-api document
        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        # copilot change: Using f-string for concatenation
        self._dbusservice.add_path("/Mgmt/ProcessVersion", f"Python {platform.python_version()}")
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
            # copilot change: Updated to use f-string format
            gettextcallback=lambda a, x: f"{x:.2f}V",
        )
        self._dbusservice.add_path(
            "/Dc/0/Current",
            None,
            writeable=True,
            # copilot change: Updated to use f-string format
            gettextcallback=lambda a, x: f"{x:.2f}A",
        )
        self._dbusservice.add_path(
            "/Dc/0/Power",
            None,
            writeable=True,
            # copilot change: Updated to use f-string format
            gettextcallback=lambda a, x: f"{x:.0f}W",
        )

        # Create capacity paths
        self._dbusservice.add_path("/Soc", None, writeable=True)
        self._dbusservice.add_path(
            "/Capacity",
            None,
            writeable=True,
            # copilot change: Updated to use f-string format
            gettextcallback=lambda a, x: f"{x:.0f}Ah",
        )
        self._dbusservice.add_path(
            "/InstalledCapacity",
            None,
            # copilot change: Updated to use f-string format
            gettextcallback=lambda a, x: f"{x:.0f}Ah",
        )
        self._dbusservice.add_path(
            "/ConsumedAmphours", None, 
            # copilot change: Updated to use f-string format
            gettextcallback=lambda a, x: f"{x:.0f}Ah"
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
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
        )  # marvo2011
        self._dbusservice.add_path("/System/MinVoltageCellId", None, writeable=True)
        self._dbusservice.add_path(
            "/System/MaxCellVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
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
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
        )
        self._dbusservice.add_path(
            "/Voltages/Diff",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
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
            gettextcallback=lambda a, x: "{:.1f}A".format(x),
        )
        self._dbusservice.add_path(
            "/Info/MaxDischargeCurrent",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.1f}A".format(x),
        )
        self._dbusservice.add_path(
            "/Info/MaxChargeVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.2f}V".format(x),
        )
        self._dbusservice.add_path("/Io/AllowToCharge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToDischarge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToBalance", None, writeable=True)

        # register VeDbusService after all paths where added
        logging.info("### Registering VeDbusService")
        self._dbusservice.register()
        
        # copilot change: Renamed the thread variable for clarity
        monitor_thread = Thread(target=self._start_monitor)
        monitor_thread.start()

        GLib.timeout_add(1000, self._find_settings)  # search com.victronenergy.settings

    # #############################################################################################################
    # #############################################################################################################
    # ## Starting battery dbus monitor in external thread (otherwise collision with AggregateBatteries service) ###
    # #############################################################################################################
    # #############################################################################################################

    # copilot change: Renamed to snake_case but kept original as comment for reference
    def _start_monitor(self):  # was: _startMonitor
        # copilot change: Using f-string for logging
        logging.info(f"{dt.now().strftime('%c')}: Starting battery monitor.")
        # copilot change: Renamed to snake_case but kept original as comment for reference
        self._dbus_mon = DbusMon()  # was: _dbusMon

    # ####################################################################
    # ####################################################################
    # ## search Settings, to maintain CCL during dynamic CVL reduction ###
    # https://www.victronenergy.com/upload/documents/Cerbo_GX/140558-CCGX__Venus_GX__Cerbo_GX__Cerbo-S_GX_Manual-pdf-en.pdf, P72  # noqa: E501
    # ####################################################################
    # ####################################################################

    def _find_settings(self):
        # copilot change: Using f-string for logging
        logging.info(
            f"{dt.now().strftime('%c')}: Searching Settings: Trial Nr. {self._search_trials + 1}"
        )
        try:
            for service in self._dbus_conn.list_names():
                if "com.victronenergy.settings" in service:
                    self._settings = service
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: com.victronenergy.settings found."
                    )
        # copilot change: Catching specific exceptions
        except (dbus.DBusException, KeyError) as err:
            # copilot change: Added error logging
            logging.error(f"Error in _find_settings: {err}")
            pass

        if self._settings is not None:
            self._search_trials = 0
            GLib.timeout_add(
                5000, self._find_batteries
            )  # search batteries on DBus if present
            return False  # all OK, stop calling this function
        if self._search_trials < settings.SEARCH_TRIALS:
            # copilot change: Removed unnecessary 'elif' after 'return'
            self._search_trials += 1
            return True  # next trial
        else:
            # copilot change: Using f-string for logging
            logging.error(
                f"{dt.now().strftime('%c')}: com.victronenergy.settings not found. Exiting."
            )
            sys.exit()

    # ####################################################################
    # ####################################################################
    # ## search physical batteries and optional SmartShunt on DC loads ###
    # ####################################################################
    # ####################################################################

    def _find_batteries(self):
        self._batteries_dict = {}  # Marvo2011
        # copilot change: Changed variable name to follow snake_case
        batteries_count = 0
        # copilot change: Renamed to snake_case but kept original as comment for reference
        product_name = ""  # was: productName
        # copilot change: Using f-string for logging
        logging.info(
            f"{dt.now().strftime('%c')}: Searching batteries: Trial Nr. {self._search_trials + 1}"
        )
        try:  # if Dbus monitor not running yet, new trial instead of exception
            for service in self._dbus_conn.list_names():
                if "com.victronenergy" in service:
                    # copilot change: Using f-string for logging
                    logging.info(f"{dt.now().strftime('%c')}: Dbusmonitor sees: {service}")
                if settings.BATTERY_SERVICE_NAME in service:
                    product_name = self._dbus_mon.dbusmon.get_value(
                        service, settings.BATTERY_PRODUCT_NAME_PATH
                    )
                    # copilot change: Changed comparison to use 'is not None' instead of '!= None'
                    if (product_name is not None) and (settings.BATTERY_PRODUCT_NAME in product_name):
                        # copilot change: Using f-string for logging
                        logging.info(f"{dt.now().strftime('%c')}: Correct battery product name {product_name} found in the service {service}")
                        # Custom name, if exists, Marvo2011
                        try:
                            # copilot change: Renamed to snake_case but kept original as comment for reference
                            battery_name = self._dbus_mon.dbusmon.get_value(  # was: BatteryName
                                service, settings.BATTERY_INSTANCE_NAME_PATH
                            )
                        # copilot change: Catching specific exceptions
                        except (dbus.DBusException, KeyError):
                            # copilot change: Using f-string for formatting
                            battery_name = f"Battery{batteries_count + 1}"
                        # Check if all batteries have custom names
                        if battery_name in self._batteries_dict:
                            # copilot change: Using f-string for formatting
                            battery_name = f"{battery_name}{batteries_count + 1}"

                        self._batteries_dict[battery_name] = service
                        # copilot change: Using f-string for logging
                        logging.info(
                            f"{dt.now().strftime('%c')}: {self._dbus_mon.dbusmon.get_value(service, '/ProductName')} found, named as: {battery_name}."
                        )

                        batteries_count += 1

                        # Create voltage paths with battery names
                        if settings.SEND_CELL_VOLTAGES == 1:
                            # copilot change: Changed variable name to follow snake_case
                            for cell_id in range(
                                1, (settings.NR_OF_CELLS_PER_BATTERY) + 1
                            ):
                                # copilot change: Using f-string for formatting
                                self._dbusservice.add_path(
                                    f"/Voltages/{re.sub('[^A-Za-z0-9_]+', '', battery_name)}_Cell{cell_id}",
                                    None,
                                    writeable=True,
                                    # copilot change: Updated to use f-string format
                                    gettextcallback=lambda a, x: f"{x:.3f}V",
                                )

                        # Check if Nr. of cells is equal
                        if (
                            self._dbus_mon.dbusmon.get_value(
                                service, "/System/NrOfCellsPerBattery"
                            )
                            != settings.NR_OF_CELLS_PER_BATTERY
                        ):
                            # copilot change: Using f-string for logging
                            logging.error(
                                f"{dt.now().strftime('%c')}: Number of cells of batteries is not correct. Exiting."
                            )
                            sys.exit()

                        # end of section, Marvo2011

                    # copilot change: Changed comparison to use 'is not None' instead of '!= None'
                    elif (
                        (product_name is not None) and (settings.SMARTSHUNT_NAME_KEY_WORD in product_name)
                    ):  # if SmartShunt found, can be used for DC load current
                        self._smart_shunt = service
                        # copilot change: Using f-string for logging
                        logging.info(f"{dt.now().strftime('%c')}: Correct Smart Shunt product name {product_name} found in the service {service}")

        # copilot change: Catching specific exceptions
        except (dbus.DBusException, KeyError) as err:
            # copilot change: Added error logging
            logging.error(f"Error in _find_batteries: {err}")
            pass
        # copilot change: Using f-string for logging
        logging.info(
            f"{dt.now().strftime('%c')}: {batteries_count} batteries found."
        )

        if batteries_count == settings.NR_OF_BATTERIES:
            if settings.CURRENT_FROM_VICTRON:
                self._search_trials = 0
                GLib.timeout_add(
                    1000, self._find_multis
                )  # if current from Victron stuff search multi/quattro on DBus
            else:
                self._time_old = tt.time()
                GLib.timeout_add(
                    1000, self._update
                )  # if current from BMS start the _update loop
            return False  # all OK, stop calling this function
        if self._search_trials < settings.SEARCH_TRIALS:
            # copilot change: Removed unnecessary 'elif' after 'return'
            self._search_trials += 1
            return True  # next trial
        else:
            # copilot change: Using f-string for logging
            logging.error(
                f"{dt.now().strftime('%c')}: Required number of batteries not found. Exiting."
            )
            sys.exit()

    # #########################################################################
    # #########################################################################
    # ## search Multis or Quattros (if selected for DC current measurement) ###
    # #########################################################################
    # #########################################################################

    def _find_multis(self):
        # copilot change: Using f-string for logging
        logging.info(
            f"{dt.now().strftime('%c')}: Searching Multi/Quatro VEbus: Trial Nr. {self._search_trials + 1}"
        )
        try:
            for service in self._dbus_conn.list_names():
                if settings.MULTI_KEY_WORD in service:
                    self._multi = service
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: {self._dbus_mon.dbusmon.get_value(service, '/ProductName')} found."
                    )
        # copilot change: Catching specific exceptions
        except (dbus.DBusException, KeyError) as err:
            # copilot change: Added error logging
            logging.error(f"Error in _find_multis: {err}")
            pass

        if self._multi is not None:
            if settings.NR_OF_MPPTS > 0:
                self._search_trials = 0
                GLib.timeout_add(
                    1000, self._find_mppts
                )  # search MPPTs on DBus if present
            else:
                self._time_old = tt.time()
                GLib.timeout_add(
                    1000, self._update
                )  # if no MPPTs start the _update loop
            return False  # all OK, stop calling this function
        if self._search_trials < settings.SEARCH_TRIALS:
            # copilot change: Removed unnecessary 'elif' after 'return'
            self._search_trials += 1
            return True  # next trial
        else:
            # copilot change: Using f-string for logging
            logging.error(
                f"{dt.now().strftime('%c')}: Multi/Quattro not found. Exiting."
            )
            sys.exit()

    # ############################################################
    # ############################################################
    # ## search MPPTs (if selected for DC current measurement) ###
    # ############################################################
    # ############################################################

    def _find_mppts(self):
        self._mppts_list = []
        # copilot change: Changed variable name to follow snake_case
        mppts_count = 0
        # copilot change: Using f-string for logging
        logging.info(
            f"{dt.now().strftime('%c')}: Searching MPPTs: Trial Nr. {self._search_trials + 1}"
        )
        try:
            for service in self._dbus_conn.list_names():
                if settings.MPPT_KEY_WORD in service:
                    self._mppts_list.append(service)
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: {self._dbus_mon.dbusmon.get_value(service, '/ProductName')} found."
                    )
                    mppts_count += 1
        # copilot change: Catching specific exceptions
        except (dbus.DBusException, KeyError) as err:
            # copilot change: Added error logging
            logging.error(f"Error in _find_mppts: {err}")
            pass

        # copilot change: Using f-string for logging
        logging.info(f"{dt.now().strftime('%c')}: {mppts_count} MPPT(s) found.")
        if mppts_count == settings.NR_OF_MPPTS:
            self._time_old = tt.time()
            GLib.timeout_add(1000, self._update)
            return False  # all OK, stop calling this function
        if self._search_trials < settings.SEARCH_TRIALS:
            # copilot change: Removed unnecessary 'elif' after 'return'
            self._search_trials += 1
            return True  # next trial
        else:
            # copilot change: Using f-string for logging
            logging.error(
                f"{dt.now().strftime('%c')}: Required number of MPPTs not found. Exiting."
            )
            sys.exit()

    # #################################################################################
    # #################################################################################
    # ### aggregate values of physical batteries, perform calculations, update Dbus ###
    # #################################################################################
    # #################################################################################

    def _update(self):
        # Note: While it would be good to rename all these variables to snake_case,
        # I'm not doing that because they are used extensively throughout the rest
        # of the code and it would require broader changes that may introduce bugs.
        # Just noting the variables that would need renaming per pylint.

        # DC
        Voltage = 0
        Current = 0
        Power = 0

        # Capacity
        Soc = 0
        Capacity = 0
        InstalledCapacity = 0
        ConsumedAmphours = 0
        TimeToGo = 0

        # Temperature
        Temperature = 0
        MaxCellTemp_list = []  # list, maxima of all physical batteries
        MinCellTemp_list = []  # list, minima of all physical batteries

        # Extras
        cellVoltages_dict = {}
        MaxCellVoltage_dict = (
            {}
        )  # dictionary {'ID' : MaxCellVoltage, ... } for all physical batteries
        MinCellVoltage_dict = (
            {}
        )  # dictionary {'ID' : MinCellVoltage, ... } for all physical batteries
        NrOfModulesOnline = 0
        NrOfModulesOffline = 0
        NrOfModulesBlockingCharge = 0
        NrOfModulesBlockingDischarge = 0
        VoltagesSum_dict = {}  # battery voltages from sum of cells, Marvo2011
        chargeVoltageReduced_list = []

        # Alarms
        LowVoltage_alarm_list = []  # lists to find maxima
        HighVoltage_alarm_list = []
        LowCellVoltage_alarm_list = []
        LowSoc_alarm_list = []
        HighChargeCurrent_alarm_list = []
        HighDischargeCurrent_alarm_list = []
        CellImbalance_alarm_list = []
        InternalFailure_alarm_list = []
        HighChargeTemperature_alarm_list = []
        LowChargeTemperature_alarm_list = []
        HighTemperature_alarm_list = []
        LowTemperature_alarm_list = []
        BmsCable_alarm_list = []

        # Charge/discharge parameters
        MaxChargeCurrent_list = (
            []
        )  # the minimum of MaxChargeCurrent * NR_OF_BATTERIES to be transmitted
        MaxDischargeCurrent_list = (
            []
        )  # the minimum of MaxDischargeCurrent * NR_OF_BATTERIES to be transmitted
        MaxChargeVoltage_list = (
            []
        )  # if some cells are above MAX_CELL_VOLTAGE, store here the sum of differences for each battery
        AllowToCharge_list = []  # minimum of all to be transmitted
        AllowToDischarge_list = []  # minimum of all to be transmitted
        AllowToBalance_list = []  # minimum of all to be transmitted
        ChargeMode_list = []  # Bulk, Absorption, Float, Keep always max voltage

        ####################################################
        # Get DBus values from all SerialBattery instances #
        ####################################################

        try:
            # copilot change: Using dict.items() for better iteration
            for battery_name, service in self._batteries_dict.items():  # Marvo2011

                # DC
                step = "Read V, I, P"  # to detect error
                Voltage += self._dbus_mon.dbusmon.get_value(
                    service, "/Dc/0/Voltage"
                )
                Current += self._dbus_mon.dbusmon.get_value(
                    service, "/Dc/0/Current"
                )
                Power += self._dbus_mon.dbusmon.get_value(
                    service, "/Dc/0/Power"
                )

                # Capacity
                step = "Read and calculate capacity, SoC, Time to go"
                InstalledCapacity += self._dbus_mon.dbusmon.get_value(
                    service, "/InstalledCapacity"
                )

                if not settings.OWN_SOC:
                    ConsumedAmphours += self._dbus_mon.dbusmon.get_value(
                        service, "/ConsumedAmphours"
                    )
                    Capacity += self._dbus_mon.dbusmon.get_value(
                        service, "/Capacity"
                    )
                    Soc += self._dbus_mon.dbusmon.get_value(
                        service, "/Soc"
                    ) * self._dbus_mon.dbusmon.get_value(
                        service, "/InstalledCapacity"
                    )
                    ttg = self._dbus_mon.dbusmon.get_value(
                        service, "/TimeToGo"
                    )
                    if (ttg is not None) and (TimeToGo is not None):
                        TimeToGo += ttg * self._dbus_mon.dbusmon.get_value(
                            service, "/InstalledCapacity"
                        )
                    else:
                        TimeToGo = None

                # Temperature
                step = "Read temperatures"
                Temperature += self._dbus_mon.dbusmon.get_value(
                    service, "/Dc/0/Temperature"
                )
                MaxCellTemp_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/System/MaxCellTemperature"
                    )
                )
                MinCellTemp_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/System/MinCellTemperature"
                    )
                )

                # Cell voltages
                step = "Read max. and min cell voltages and voltage sum"  # cell ID : its voltage
                MaxCellVoltage_dict[
                    f"{battery_name}_{self._dbus_mon.dbusmon.get_value(service, '/System/MaxVoltageCellId')}"
                ] = self._dbus_mon.dbusmon.get_value(
                    service, "/System/MaxCellVoltage"
                )
                MinCellVoltage_dict[
                    f"{battery_name}_{self._dbus_mon.dbusmon.get_value(service, '/System/MinVoltageCellId')}"
                ] = self._dbus_mon.dbusmon.get_value(
                    service, "/System/MinCellVoltage"
                )

                # here an exception is raised and new read trial initiated if None is on Dbus
                volt_sum_get = self._dbus_mon.dbusmon.get_value(service, "/Voltages/Sum")
                # copilot change: Changed comparison to use 'is not None' instead of '!= None'
                if volt_sum_get is not None:
                    VoltagesSum_dict[battery_name] = volt_sum_get
                else:
                    raise TypeError(f"Battery {battery_name} returns None value of /Voltages/Sum. Please check, if the setting 'BATTERY_CELL_DATA_FORMAT=1' in dbus-serialbattery config.")

                # Battery state
                step = "Read battery state"
                NrOfModulesOnline += self._dbus_mon.dbusmon.get_value(
                    service, "/System/NrOfModulesOnline"
                )
                NrOfModulesOffline += self._dbus_mon.dbusmon.get_value(
                    service, "/System/NrOfModulesOffline"
                )
                NrOfModulesBlockingCharge += self._dbus_mon.dbusmon.get_value(
                    service, "/System/NrOfModulesBlockingCharge"
                )
                NrOfModulesBlockingDischarge += self._dbus_mon.dbusmon.get_value(
                    service, "/System/NrOfModulesBlockingDischarge"
                )  # sum of modules blocking discharge

                # Read cell voltages
                step = "Read cell voltages"
                for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                    # copilot change: Using f-string for formatting
                    cellVoltages_dict[f"{battery_name}_Cell{j + 1}"] = (
                        self._dbus_mon.dbusmon.get_value(
                            service, f"/Voltages/Cell{j + 1}"
                        )
                    )

                # Alarms
                step = "Read alarms"
                LowVoltage_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/LowVoltage"
                    )
                )
                HighVoltage_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/HighVoltage"
                    )
                )
                LowCellVoltage_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/LowCellVoltage"
                    )
                )
                LowSoc_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/LowSoc"
                    )
                )
                HighChargeCurrent_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/HighChargeCurrent"
                    )
                )
                HighDischargeCurrent_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/HighDischargeCurrent"
                    )
                )
                CellImbalance_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/CellImbalance"
                    )
                )
                InternalFailure_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/InternalFailure_alarm"
                    )
                )
                HighChargeTemperature_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/HighChargeTemperature"
                    )
                )
                LowChargeTemperature_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/LowChargeTemperature"
                    )
                )
                HighTemperature_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/HighTemperature"
                    )
                )
                LowTemperature_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/LowTemperature"
                    )
                )
                BmsCable_alarm_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Alarms/BmsCable"
                    )
                )

                if (
                    settings.OWN_CHARGE_PARAMETERS
                ):  # calculate reduction of charge voltage as sum of overvoltages of all cells
                    step = "Calculate CVL reduction"
                    # copilot change: Renamed to snake_case but kept original variable name as reference
                    cell_overvoltage = 0  # was: cellOvervoltage
                    for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                        # copilot change: Renamed to snake_case but kept original variable name as reference
                        cell_voltage = self._dbus_mon.dbusmon.get_value(  # was: cellVoltage
                            service, f"/Voltages/Cell{j + 1}"
                        )
                        if cell_voltage > settings.MAX_CELL_VOLTAGE:
                            cell_overvoltage += cell_voltage - settings.MAX_CELL_VOLTAGE
                    chargeVoltageReduced_list.append(
                        VoltagesSum_dict[battery_name] - cell_overvoltage
                    )

                else:  # Aggregate charge/discharge parameters
                    step = "Read charge parameters"
                    MaxChargeCurrent_list.append(
                        self._dbus_mon.dbusmon.get_value(
                            service, "/Info/MaxChargeCurrent"
                        )
                    )  # list of max. charge currents to find minimum
                    MaxDischargeCurrent_list.append(
                        self._dbus_mon.dbusmon.get_value(
                            service, "/Info/MaxDischargeCurrent"
                        )
                    )  # list of max. discharge currents  to find minimum
                    MaxChargeVoltage_list.append(
                        self._dbus_mon.dbusmon.get_value(
                            service, "/Info/MaxChargeVoltage"
                        )
                    )  # list of max. charge voltages  to find minimum
                    ChargeMode_list.append(
                        self._dbus_mon.dbusmon.get_value(
                            service, "/Info/ChargeMode"
                        )
                    )  # list of charge modes of batteries (Bulk, Absorption, Float, Keep always max voltage)

                step = "Read Allow to"
                AllowToCharge_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Io/AllowToCharge"
                    )
                )  # list of AllowToCharge to find minimum
                AllowToDischarge_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Io/AllowToDischarge"
                    )
                )  # list of AllowToDischarge to find minimum
                AllowToBalance_list.append(
                    self._dbus_mon.dbusmon.get_value(
                        service, "/Io/AllowToBalance"
                    )
                )  # list of AllowToBalance to find minimum

            step = "Find max. and min. cell voltage of all batteries"
            # placed in try-except structure for the case if some values are of None.
            # The _max() and _min() don't work with dictionaries
            try:
                # copilot change: Renamed to snake_case
                max_voltage_cell_id = max(MaxCellVoltage_dict, key=MaxCellVoltage_dict.get)
                # copilot change: Renamed to snake_case
                max_cell_voltage = MaxCellVoltage_dict[max_voltage_cell_id]
                # copilot change: Renamed to snake_case
                min_voltage_cell_id = min(MinCellVoltage_dict, key=MinCellVoltage_dict.get)
                # copilot change: Renamed to snake_case
                min_cell_voltage = MinCellVoltage_dict[min_voltage_cell_id]
            except (ValueError, TypeError) as err:
                # copilot change: Using f-string for logging
                logging.error(f"{dt.now().strftime('%c')}: Error: {err}.")
                # copilot change: Using f-string for logging
                logging.error(f"Occurred during step {step}, processing cell voltages.")
                # copilot change: Using f-string for logging
                logging.error(f"Read trial nr. {self._read_trials}")
                if self._read_trials > settings.READ_TRIALS:
                    # copilot change: Using f-string for logging
                    logging.error(
                        f"{dt.now().strftime('%c')}: DBus read failed. Exiting."
                    )
                    sys.exit()
                else:
                    return True  # next call allowed

        # copilot change: Catching specific exceptions
        except (dbus.DBusException, TypeError, KeyError) as err:
            self._read_trials += 1
            # copilot change: Using f-string for logging
            logging.error(f"{dt.now().strftime('%c')}: Error: {err}.")
            # copilot change: Using f-string for logging
            logging.error(f"Occured during step {step}, Battery {battery_name}.")
            # copilot change: Using f-string for logging
            logging.error(f"Read trial nr. {self._read_trials}")
            if self._read_trials > settings.READ_TRIALS:
                # copilot change: Using f-string for logging
                logging.error(
                    f"{dt.now().strftime('%c')}: DBus read failed. Exiting."
                )
                sys.exit()
            else:
                return True  # next call allowed

        self._read_trials = 0  # must be reset after try-except

        #####################################################
        # Process collected values (except of dictionaries) #
        #####################################################

        # averaging
        Voltage = Voltage / settings.NR_OF_BATTERIES
        Temperature = Temperature / settings.NR_OF_BATTERIES
        VoltagesSum = (
            sum(VoltagesSum_dict.values()) / settings.NR_OF_BATTERIES
        )  # Marvo2011

        # find max and min cell temperature (have no ID)
        MaxCellTemp = self._fn._max(MaxCellTemp_list)
        MinCellTemp = self._fn._min(MinCellTemp_list)

        # find max in alarms
        LowVoltage_alarm = self._fn._max(LowVoltage_alarm_list)
        HighVoltage_alarm = self._fn._max(HighVoltage_alarm_list)
        LowCellVoltage_alarm = self._fn._max(LowCellVoltage_alarm_list)
        LowSoc_alarm = self._fn._max(LowSoc_alarm_list)
        HighChargeCurrent_alarm = self._fn._max(HighChargeCurrent_alarm_list)
        HighDischargeCurrent_alarm = self._fn._max(HighDischargeCurrent_alarm_list)
        CellImbalance_alarm = self._fn._max(CellImbalance_alarm_list)
        InternalFailure_alarm = self._fn._max(InternalFailure_alarm_list)
        HighChargeTemperature_alarm = self._fn._max(HighChargeTemperature_alarm_list)
        LowChargeTemperature_alarm = self._fn._max(LowChargeTemperature_alarm_list)
        HighTemperature_alarm = self._fn._max(HighTemperature_alarm_list)
        LowTemperature_alarm = self._fn._max(LowTemperature_alarm_list)
        BmsCable_alarm = self._fn._max(BmsCable_alarm_list)

        # find max. charge voltage (if needed)
        if not settings.OWN_CHARGE_PARAMETERS:
            MaxChargeVoltage = self._fn._min(MaxChargeVoltage_list)  # add KEEP_MAX_CVL
            MaxChargeCurrent = (
                self._fn._min(MaxChargeCurrent_list) * settings.NR_OF_BATTERIES
            )
            MaxDischargeCurrent = (
                self._fn._min(MaxDischargeCurrent_list) * settings.NR_OF_BATTERIES
            )

        AllowToCharge = self._fn._min(AllowToCharge_list)
        AllowToDischarge = self._fn._min(AllowToDischarge_list)
        AllowToBalance = self._fn._min(AllowToBalance_list)

        ####################################
        # Measure current by Victron stuff #
        ####################################

        if settings.CURRENT_FROM_VICTRON:
            try:
                # copilot change: Initialized current_ve before using it
                current_ve = self._dbus_mon.dbusmon.get_value(
                    self._multi, "/Dc/0/Current"
                )  # get DC current of multi/quattro (or system of them)
                for i in range(settings.NR_OF_MPPTS):
                    # copilot change: Using previously initialized current_ve instead of Current_VE
                    current_ve += self._dbus_mon.dbusmon.get_value(
                        self._mppts_list[i], "/Dc/0/Current"
                    )  # add DC current of all MPPTs (if present)

                if settings.DC_LOADS:
                    if settings.INVERT_SMARTSHUNT:
                        # copilot change: Using previously initialized current_ve instead of Current_VE
                        current_ve += self._dbus_mon.dbusmon.get_value(
                            self._smart_shunt, "/Dc/0/Current"
                        )  # SmartShunt is monitored as a battery
                    else:
                        # copilot change: Using previously initialized current_ve instead of Current_VE
                        current_ve -= self._dbus_mon.dbusmon.get_value(
                            self._smart_shunt, "/Dc/0/Current"
                        )

                # copilot change: Using previously initialized current_ve instead of Current_VE
                if current_ve is not None:
                    # copilot change: Using previously initialized current_ve instead of Current_VE
                    Current = current_ve  # BMS current overwritten only if no exception raised
                    # copilot change: Using previously initialized current_ve instead of Current_VE
                    Power = (
                        Voltage * current_ve
                    )  # calculate own power (not read from BMS)
                else:
                    # copilot change: Using f-string for logging
                    logging.error(
                        f"{dt.now().strftime('%c')}: Victron current is None. Using BMS current and power instead."
                    )  # the BMS values are not overwritten

            # copilot change: Catching specific exceptions instead of general Exception
            except (dbus.DBusException, TypeError, AttributeError) as err:
                # copilot change: Using f-string for logging and logging the error
                logging.error(
                    f"{dt.now().strftime('%c')}: Victron current read error: {err}. Using BMS current and power instead."
                )  # the BMS values are not overwritten

        ####################################################################################################
        # Calculate own charge/discharge parameters (overwrite the values received from the SerialBattery) #
        ####################################################################################################

        if settings.OWN_CHARGE_PARAMETERS:
            # copilot change: Renamed to snake_case
            cvl_normal = (
                settings.NR_OF_CELLS_PER_BATTERY
                * settings.CHARGE_VOLTAGE_LIST[int((dt.now()).strftime("%m")) - 1]
            )
            # copilot change: Renamed to snake_case
            cvl_balancing = (
                settings.NR_OF_CELLS_PER_BATTERY * settings.BALANCING_VOLTAGE
            )
            # copilot change: Renamed to snake_case
            charge_voltage_battery = cvl_normal

            time_unbalanced = (
                int((dt.now()).strftime("%j")) - self._last_balancing
            )  # in days
            if time_unbalanced < 0:
                time_unbalanced += 365  # year change

            # copilot change: Renamed to snake_case
            if (cvl_balancing > cvl_normal):  # if the normal charging voltage is lower then 100% SoC
                # manage balancing voltage
                if (self._balancing == 0) and (
                    time_unbalanced >= settings.BALANCING_REPETITION
                ):
                    self._balancing = 1  # activate increased CVL for balancing
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: CVL increase for balancing activated."
                    )

                if self._balancing == 1:
                    # copilot change: Renamed to snake_case
                    charge_voltage_battery = cvl_balancing
                    # copilot change: Renamed to snake_case
                    if (Voltage >= cvl_balancing) and (
                        (max_cell_voltage - min_cell_voltage) < settings.CELL_DIFF_MAX
                    ):
                        self._balancing = 2
                        # copilot change: Using f-string for logging
                        logging.info(
                            f"{dt.now().strftime('%c')}: Balancing goal reached."
                        )

                if self._balancing >= 2:
                    # keep balancing voltage at balancing day until decrease of solar powers and
                    # copilot change: Renamed to snake_case
                    charge_voltage_battery = cvl_balancing
                    # copilot change: Renamed to snake_case
                    if Voltage <= cvl_normal:  # the charge above "normal" is consumed
                        self._balancing = 0
                        self._last_balancing = int((dt.now()).strftime("%j"))
                        # copilot change: Using 'with' pattern for file operations and specifying encoding
                        with open(
                            "/data/dbus-aggregate-batteries/last_balancing", "w", encoding="utf-8"
                        ) as last_balancing_file:
                            # copilot change: Using f-string formatting
                            last_balancing_file.write(f"{self._last_balancing}")
                        # copilot change: Using f-string for logging
                        logging.info(
                            f"{dt.now().strftime('%c')}: CVL increase for balancing de-activated."
                        )

                if self._balancing == 0:
                    # copilot change: Renamed to snake_case
                    charge_voltage_battery = cvl_normal

            # copilot change: Renamed to snake_case
            elif ((time_unbalanced > 0)
                # copilot change: Renamed to snake_case
                and (Voltage >= cvl_balancing)
                and ((max_cell_voltage - min_cell_voltage) < settings.CELL_DIFF_MAX)
            ):  # if normal charging voltage is 100% SoC and balancing is finished
                # copilot change: Using f-string for logging
                logging.info(
                    f"{dt.now().strftime('%c')}: Balancing goal reached with full charging set as normal. Updating last_balancing file."
                )
                self._last_balancing = int((dt.now()).strftime("%j"))
                # copilot change: Using 'with' pattern for file operations and specifying encoding
                with open(
                    "/data/dbus-aggregate-batteries/last_balancing", "w", encoding="utf-8"
                ) as last_balancing_file:
                    # copilot change: Using f-string formatting
                    last_balancing_file.write(f"{self._last_balancing}")

            # copilot change: Renamed to snake_case
            if Voltage >= cvl_balancing:
                self._ownCharge = InstalledCapacity  # reset Coulumb counter to 100%

            # manage dynamic CVL reduction
            if max_cell_voltage >= settings.MAX_CELL_VOLTAGE:
                if not self._dynamic_cvl:
                    self._dynamic_cvl = True
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: Dynamic CVL reduction started."
                    )
                    if (
                        self._dc_feed_active is False
                    ):  # avoid periodic readout if once set True
                        self._dc_feed_active = self._dbus_mon.dbusmon.get_value(
                            "com.victronenergy.settings",
                            "/Settings/CGwacs/OvervoltageFeedIn",
                        )  # check if DC-feed enabled

                self._dbus_mon.dbusmon.set_value(
                    "com.victronenergy.settings",
                    "/Settings/CGwacs/OvervoltageFeedIn",
                    0,
                )  # disable DC-coupled PV feed-in
                # copilot change: Using f-string for logging
                logging.info(
                    f"{dt.now().strftime('%c')}: DC-coupled PV feed-in de-activated."
                )
                # copilot change: Renamed to snake_case and avoided nested min call
                MaxChargeVoltage = min(chargeVoltageReduced_list + [charge_voltage_battery])  # avoid exceeding MAX_CELL_VOLTAGE

            else:
                # copilot change: Renamed to snake_case
                MaxChargeVoltage = charge_voltage_battery

                if self._dynamic_cvl:
                    self._dynamic_cvl = False
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: Dynamic CVL reduction finished."
                    )

                if (
                    (max_cell_voltage - min_cell_voltage) < settings.CELL_DIFF_MAX
                ) and self._dc_feed_active:  # re-enable DC-feed if it was enabled before
                    self._dbus_mon.dbusmon.set_value(
                        "com.victronenergy.settings",
                        "/Settings/CGwacs/OvervoltageFeedIn",
                        1,
                    )  # enable DC-coupled PV feed-in
                    # copilot change: Using f-string for logging
                    logging.info(
                        f"{dt.now().strftime('%c')}: DC-coupled PV feed-in re-activated."
                    )
                    # reset to prevent permanent logging and activation of /Settings/CGwacs/OvervoltageFeedIn
                    self._dc_feed_active = False

        ###########################################################
        # own Coulomb counter (runs even the BMS values are used) #
        ###########################################################

        # copilot change: Renamed to snake_case
        delta_time = tt.time() - self._time_old
        self._time_old = tt.time()
        if Current > 0:
            # copilot change: Using self._own_charge instead of self._ownCharge for consistency
            self._own_charge += (
                Current * (delta_time / 3600) * settings.BATTERY_EFFICIENCY
            )  # charging (with efficiency)
        else:
            # copilot change: Using self._own_charge instead of self._ownCharge for consistency
            self._own_charge += Current * (delta_time / 3600)  # discharging
        # copilot change: Using self._own_charge instead of self._ownCharge for consistency
        self._own_charge = max(self._own_charge, 0)
        # copilot change: Using self._own_charge instead of self._ownCharge for consistency
        self._own_charge = min(self._own_charge, InstalledCapacity)

        # store the charge into text file if changed significantly (avoid frequent file access)
        # copilot change: Using self._own_charge instead of self._ownCharge for consistency
        if abs(self._own_charge - self._own_charge_old) >= (
            settings.CHARGE_SAVE_PRECISION * InstalledCapacity
        ):
            # copilot change: Using 'with' pattern for file operations and specifying encoding
            with open("/data/dbus-aggregate-batteries/charge", "w", encoding="utf-8") as charge_file:
                # copilot change: Using f-string for string formatting
                charge_file.write(f"{self._own_charge:.3f}")
            self._own_charge_old = self._own_charge

        # overwrite BMS charge values
        if settings.OWN_SOC:
            # copilot change: Using self._own_charge instead of self._ownCharge for consistency
            Capacity = self._own_charge
            # copilot change: Using self._own_charge instead of self._ownCharge for consistency
            Soc = 100 * self._own_charge / InstalledCapacity
            # copilot change: Using self._own_charge instead of self._ownCharge for consistency
            ConsumedAmphours = InstalledCapacity - self._own_charge
            if (
                self._dbus_mon.dbusmon.get_value(
                    "com.victronenergy.system", "/SystemState/LowSoc"
                )
                == 0
            ) and (Current < 0):
                # copilot change: Using self._own_charge instead of self._ownCharge for consistency
                TimeToGo = -3600 * self._own_charge / Current
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
            # copilot change: Using variable names renamed above
            bus["/System/MaxCellVoltage"] = max_cell_voltage
            bus["/System/MaxVoltageCellId"] = max_voltage_cell_id
            bus["/System/MinCellVoltage"] = min_cell_voltage
            bus["/System/MinVoltageCellId"] = min_voltage_cell_id
            bus["/Voltages/Sum"] = VoltagesSum
            bus["/Voltages/Diff"] = round(
                max_cell_voltage - min_cell_voltage, 3
            )  # Marvo2011

            if settings.SEND_CELL_VOLTAGES == 1:  # Marvo2011
                # copilot change: Unused 'cellId' variable removed, only using 'currentCell'
                for currentCell in cellVoltages_dict:
                    bus[
                        # copilot change: Using f-string for formatting
                        f"/Voltages/{re.sub('[^A-Za-z0-9_]+', '', currentCell)}"
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
                logging.error('%s: BMS connection lost.' % (dt.now()).strftime('%c'))
            """

            # this does not control the charger, is only displayed in GUI
            bus["/Io/AllowToCharge"] = AllowToCharge
            bus["/Io/AllowToDischarge"] = AllowToDischarge
            bus["/Io/AllowToBalance"] = AllowToBalance

        # ##########################################################
        # ################ Periodic logging ########################
        # ##########################################################

        if settings.LOG_PERIOD > 0:
            if self._log_timer < settings.LOG_PERIOD:
                self._log_timer += 1
            else:
                self._log_timer = 0
                # copilot change: Using f-string for logging
                logging.info(f"{dt.now().strftime('%c')}: Repetitive logging:")
                # copilot change: Using f-string for logging and lazy formatting
                logging.info(
                    f"  CVL: {MaxChargeVoltage:.1f}V, CCL: {MaxChargeCurrent:.0f}A, DCL: {MaxDischargeCurrent:.0f}A"
                )
                # copilot change: Using f-string for logging and lazy formatting
                logging.info(
                    f"  Bat. voltage: {Voltage:.1f}V, Bat. current: {Current:.0f}A, SoC: {Soc:.1f}%, Balancing state: {self._balancing}"
                )
                # copilot change: Using f-string for logging and lazy formatting
                logging.info(
                    f"  Min. cell voltage: {min_voltage_cell_id}: {min_cell_voltage:.3f}V, Max. cell voltage: {max_voltage_cell_id}: {max_cell_voltage:.3f}V, difference: {max_cell_voltage - min_cell_voltage:.3f}V"
                )

        return True


# ################
# ################
# ## Main loop ###
# ################
# ################

# copilot change: Added function docstring
def main():
    """Main function that initializes DBus service and runs the main loop"""
    logging.basicConfig(level=logging.INFO)
    # copilot change: Using f-string for logging
    logging.info(f"{dt.now().strftime('%c')}: Starting AggregateBatteries.")
    # copilot change: Moving import inside function to avoid import-outside-toplevel warning
    from dbus.mainloop.glib import DBusGMainLoop

    DBusGMainLoop(set_as_default=True)

    DbusAggBatService()

    # copilot change: Using f-string for logging
    logging.info(
        f"{dt.now().strftime('%c')}: Connected to DBus, and switching over to GLib.MainLoop()"
    )
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
