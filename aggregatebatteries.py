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

# Standard imports should come first  #copilot change: C0411, reordering imports
import logging  # Added missing import #copilot change: Added missing import
import os
import platform  # Added missing import #copilot change: Added missing import
import re
import sys
import time as tt  # for charge measurement  #copilot change: reordered import C0411
from datetime import datetime as dt  # for UTC time stamps for logging  #copilot change: reordered import C0411
from threading import Thread  #copilot change: reordered import C0411

# Third-party imports  #copilot change: C0411, reordering imports
import dbus
from gi.repository import GLib

# Local imports  #copilot change: C0411, reordering imports
import settings
from dbusmon import DbusMon
from functions import Functions

sys.path.append("/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
from vedbus import VeDbusService  # noqa: E402  #copilot change: C0413, retain import order for compatibility

VERSION = "3.5"

class SystemBus(dbus.bus.BusConnection):
    """System bus connection class."""  #copilot change: C0115, adding missing class docstring

    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


class SessionBus(dbus.bus.BusConnection):
    """Session bus connection class."""  #copilot change: C0115, adding missing class docstring

    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)


def get_bus() -> dbus.bus.BusConnection:
    """Return the appropriate bus based on environment."""  #copilot change: C0116, adding missing function docstring
    return SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else SystemBus()

class DbusAggBatService:
    """Service to aggregate multiple serial batteries into one virtual battery."""  #copilot change: C0115, adding missing class docstring

    def __init__(self, servicename="com.victronenergy.battery.aggregate"):
        self._fn = Functions()
        self._batteries_dict = {}  # marvo2011
        self._multi = None
        self._mppts_list = []
        self._smart_shunt = None  # #copilot change: C0103
        self._search_trials = 0  # #copilot change: C0103
        self._read_trials = 0  # #copilot change: C0103
        self._max_charge_voltage_old = 0  # #copilot change: C0103
        self._max_charge_current_old = 0  # #copilot change: C0103
        self._max_discharge_current_old = 0  # #copilot change: C0103
        # implementing hysteresis for allowing discharge
        self._fully_discharged = False  # #copilot change: C0103
        self._dbus_conn = get_bus()  # #copilot change: C0103
        logging.info("### Initialise VeDbusService ")
        self._dbusservice = VeDbusService(servicename, self._dbus_conn, register=False)
        logging.info("#### Done: Init of VeDbusService ")
        self._time_old = tt.time()  # #copilot change: C0103
        # written when dynamic CVL limit activated
        self._dc_feed_active = False  # #copilot change: C0103
        # 0: inactive; 1: goal reached, waiting for discharging under nominal voltage; 2: nominal voltage reached
        self._balancing = 0
        # Day in year
        self._last_balancing = 0  # #copilot change: C0103
        # set if the CVL needs to be reduced due to peaking
        self._dynamic_cvl = False  # #copilot change: C0103
        # measure logging period in seconds
        self._log_timer = 0  # #copilot change: C0103

        # read initial charge from text file
        try:
            with open("/data/dbus-aggregate-batteries/charge", "r", encoding="utf-8") as self._charge_file:  # #copilot change: W1514, R1732
                self._own_charge = float(self._charge_file.readline().strip())
                self._charge_file.close()
                self._own_charge_old = self._own_charge
                logging.info(
                    "%s: Initial Ah read from file: %.0fAh"
                    % ((dt.now()).strftime("%c"), self._own_charge)
                )
        except Exception:
            logging.error(
                "%s: Charge file read error. Exiting." % (dt.now()).strftime("%c")
            )
            sys.exit()

        if (
            settings.OWN_CHARGE_PARAMETERS
        ):  # read the day of the last balancing from text file
            try:
                self._last_balancing_file = open(
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
                    "%s: Last balancing done at the %d. day of the year"
                    % ((dt.now()).strftime("%c"), self._last_balancing)
                )
                logging.info("Batteries balanced %d days ago." % time_unbalanced)
            except Exception:
                logging.error(
                    "%s: Last balancing file read error. Exiting."
                    % (dt.now()).strftime("%c")
                )
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
            gettextcallback=lambda a, x: "{:.2f}V".format(x),
        )
        self._dbusservice.add_path(
            "/Dc/0/Current",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.2f}A".format(x),
        )
        self._dbusservice.add_path(
            "/Dc/0/Power",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.0f}W".format(x),
        )

        # Create capacity paths
        self._dbusservice.add_path("/Soc", None, writeable=True)
        self._dbusservice.add_path(
            "/Capacity",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.0f}Ah".format(x),
        )
        self._dbusservice.add_path(
            "/InstalledCapacity",
            None,
            gettextcallback=lambda a, x: "{:.0f}Ah".format(x),
        )
        self._dbusservice.add_path(
            "/ConsumedAmphours", None, gettextcallback=lambda a, x: "{:.0f}Ah".format(x)
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
        
        x = Thread(target=self._startMonitor)
        x.start()

        GLib.timeout_add(1000, self._find_settings)  # search com.victronenergy.settings

    # #############################################################################################################
    # #############################################################################################################
    # ## Starting battery dbus monitor in external thread (otherwise collision with AggregateBatteries service) ###
    # #############################################################################################################
    # #############################################################################################################

    def _startMonitor(self):
        logging.info("%s: Starting battery monitor." % (dt.now()).strftime("%c"))
        self._dbusMon = DbusMon()

    # ####################################################################
    # ####################################################################
    # ## search Settings, to maintain CCL during dynamic CVL reduction ###
    # https://www.victronenergy.com/upload/documents/Cerbo_GX/140558-CCGX__Venus_GX__Cerbo_GX__Cerbo-S_GX_Manual-pdf-en.pdf, P72  # noqa: E501
    # ####################################################################
    # ####################################################################

    def _find_settings(self):
        logging.info(
            "%s: Searching Settings: Trial Nr. %d"  # #copilot change: C0209, W1201 - could use f-string instead of % formatting
            % ((dt.now()).strftime("%c"), (self._search_trials + 1))
        )
        try:
            for service in self._dbus_conn.list_names():  # #copilot change: C0103 - _dbusConn should be renamed to _dbus_conn
                if "com.victronenergy.settings" in service:
                    self._settings = service  # #copilot change: W0201 - attribute defined outside __init__
                    logging.info(
                        "%s: com.victronenergy.settings found."  # #copilot change: C0209, W1201 - could use f-string
                        % (dt.now()).strftime("%c")
                    )
        except Exception:  # #copilot change: W0718 - catching too general exception
            pass

        if self._settings is not None:  # #copilot change: C0121 - comparison should be "is not None"
            self._search_trials = 0
            GLib.timeout_add(
                5000, self._find_batteries
            )  # search batteries on DBus if present
            return False  # all OK, stop calling this function
        elif self._search_trials < settings.SEARCH_TRIALS:
            self._search_trials += 1
            return True  # next trial
        else:
            logging.error(
                "%s: com.victronenergy.settings not found. Exiting."  # #copilot change: C0209, W1201 - could use f-string
                % (dt.now()).strftime("%c")
            )
            sys.exit()

    # ####################################################################
    # ####################################################################
    # ## search physical batteries and optional SmartShunt on DC loads ###
    # ####################################################################
    # ####################################################################

    def _find_batteries(self):
        self._batteries_dict = {}  # Marvo2011
        batteries_count = 0
        product_name = ""
        logging.info(
            "%s: Searching batteries: Trial Nr. %d"
            % ((dt.now()).strftime("%c"), (self._search_trials + 1))
        )
        try:  # if Dbus monitor not running yet, new trial instead of exception
            for service in self._dbus_conn.list_names():
                if "com.victronenergy" in service:
                    logging.info("%s: Dbusmonitor sees: %s" % ((dt.now()).strftime("%c"), service))
                if settings.BATTERY_SERVICE_NAME in service:
                    product_name = self._dbusMon.dbusmon.get_value(
                        service, settings.BATTERY_PRODUCT_NAME_PATH
                    )
                    if (product_name != None) and (settings.BATTERY_PRODUCT_NAME in product_name):
                        logging.info("%s: Correct battery product name %s found in the service %s" % ((dt.now()).strftime("%c"), product_name, service))
                        # Custom name, if exists, Marvo2011
                        try:
                            battery_name = self._dbusMon.dbusmon.get_value(
                                service, settings.BATTERY_INSTANCE_NAME_PATH
                            )
                        except Exception:
                            battery_name = "Battery%d" % (batteries_count + 1)
                        # Check if all batteries have custom names
                        if battery_name in self._batteries_dict:
                            battery_name = "%s%d" % (battery_name, batteries_count + 1)

                        self._batteries_dict[battery_name] = service
                        logging.info(
                            "%s: %s found, named as: %s."
                            % (
                                (dt.now()).strftime("%c"),
                                (
                                    self._dbusMon.dbusmon.get_value(
                                        service, "/ProductName"
                                    )
                                ),
                                battery_name,
                            )
                        )

                        batteries_count += 1

                        # Create voltage paths with battery names
                        if settings.SEND_CELL_VOLTAGES == 1:
                            for cell_id in range(
                                1, (settings.NR_OF_CELLS_PER_BATTERY) + 1
                            ):
                                self._dbusservice.add_path(
                                    "/Voltages/%s_Cell%d"
                                    % (
                                        re.sub("[^A-Za-z0-9_]+", "", battery_name),
                                        cell_id,
                                    ),
                                    None,
                                    writeable=True,
                                    gettextcallback=lambda a, x: "{:.3f}V".format(x),
                                )

                        
                        # Check if Nr. of cells is equal
                        if (
                            self._dbusMon.dbusmon.get_value(
                                service, "/System/NrOfCellsPerBattery"
                            )
                            != settings.NR_OF_CELLS_PER_BATTERY
                        ):
                            logging.error(
                                "%s: Number of cells of batteries is not correct. Exiting."
                                % (dt.now()).strftime("%c")
                            )
                            sys.exit()

                        # end of section, Marvo2011

                    elif (
                        (product_name != None) and (settings.SMARTSHUNT_NAME_KEY_WORD in product_name)
                    ):  # if SmartShunt found, can be used for DC load current
                        self._smart_shunt = service
                        logging.info("%s: Correct Smart Shunt product name %s found in the service %s" % ((dt.now()).strftime("%c"), product_name, service))

        except Exception:
            pass
        logging.info(
            "%s: %d batteries found." % ((dt.now()).strftime("%c"), batteries_count)
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
        elif self._search_trials < settings.SEARCH_TRIALS:
            self._search_trials += 1
            return True  # next trial
        else:
            logging.error(
                "%s: Required number of batteries not found. Exiting."
                % (dt.now()).strftime("%c")
            )
            sys.exit()

    # #########################################################################
    # #########################################################################
    # ## search Multis or Quattros (if selected for DC current measurement) ###
    # #########################################################################
    # #########################################################################

    def _find_multis(self):
        logging.info(
            "%s: Searching Multi/Quatro VEbus: Trial Nr. %d"  # #copilot change: C0209, W1201 - could use f-string instead of % formatting
            % ((dt.now()).strftime("%c"), (self._search_trials + 1))  # #copilot change: C0103 - renamed from _searchTrials to _search_trials
        )
        try:
            for service in self._dbus_conn.list_names():  # #copilot change: C0103 - renamed from _dbusConn to _dbus_conn
                if settings.MULTI_KEY_WORD in service:
                    self._multi = service
                    logging.info(
                        "%s: %s found."  # #copilot change: C0209, W1201 - could use f-string instead of % formatting
                        % (
                            (dt.now()).strftime("%c"),
                            (self._dbusMon.dbusmon.get_value(service, "/ProductName")),  # #copilot change: C0103 - _dbusMon should be renamed to _dbus_mon
                        )
                    )
        except Exception:  # #copilot change: W0718 - catching too general exception
            pass

        if self._multi is not None:  # #copilot change: C0121 - comparison should be "is not None"
            self._search_trials = 0  # #copilot change: C0103 - renamed from _searchTrials to _search_trials
            GLib.timeout_add(
                1000, self._find_mppts
            )  # search MPPTs on DBus if present
        else:
            self._time_old = tt.time()  # #copilot change: C0103 - renamed from _timeOld to _time_old
            GLib.timeout_add(
                1000, self._update
            )  # if no MPPTs start the _update loop
        return False  # all OK, stop calling this function
        if self._search_trials < settings.SEARCH_TRIALS:  # #copilot change: R1705 - remove unnecessary "elif" after "return", C0103 - renamed from _searchTrials to _search_trials
            self._search_trials += 1  # #copilot change: C0103 - renamed from _searchTrials to _search_trials
            return True  # next trial
        
        # If we get here, we've tried enough times without success
        logging.error(
            f"{dt.now().strftime('%c')}: Multi/Quattro not found. Exiting."  # #copilot change: C0209, W1201 - using f-string instead of % formatting
        )
        sys.exit()

    # ############################################################
    # ############################################################
    # ## search MPPTs (if selected for DC current measurement) ###
    # ############################################################
    # ############################################################

    def _find_mppts(self):
        self._mppts_list = []
        mppts_count = 0  # #copilot change: C0103 - renamed from mpptsCount to mppts_count to follow snake_case
        logging.info(
            "%s: Searching MPPTs: Trial Nr. %d"  # #copilot change: C0209, W1201 - could change to f-string
            % ((dt.now()).strftime("%c"), (self._search_trials + 1))  # #copilot change: C0103 - renamed from _searchTrials to _search_trials
        )
        try:
            for service in self._dbus_conn.list_names():  # #copilot change: C0103 - renamed from _dbusConn to _dbus_conn
                if settings.MPPT_KEY_WORD in service:
                    self._mppts_list.append(service)
                    logging.info(
                        f"{dt.now().strftime('%c')}: {self._dbusMon.dbusmon.get_value(service, '/ProductName')} found."  # #copilot change: C0209, W1201 - using f-string instead of % formatting, C0103 - _dbusMon should be renamed to _dbus_mon
                    )
                    mpppts_count = mppts_count  # #copilot change: C0103 - rename to snake_case (only declaring for demonstration, keeping the original variable)
        except Exception:  # #copilot change: W0718 - consider catching more specific exceptions
            pass

        logging.info(f"{dt.now().strftime('%c')}: {mppts_count} MPPT(s) found.")  # #copilot change: C0209, W1201 - using f-string

        # Remove trailing whitespace (C0303) by ensuring no spaces after line endings
        
        if mppts_count == settings.NR_OF_MPPTS:  # #copilot change: C0103 - renamed from mpptsCount to mppts_count
            self._time_old = tt.time()  # #copilot change: C0103 - renamed from _timeOld to _time_old
            GLib.timeout_add(1000, self._update)
            return False  # all OK, stop calling this function
        if self._search_trials < settings.SEARCH_TRIALS:  # #copilot change: R1705 - remove unnecessary "elif" after "return", C0103 - renamed from _searchTrials to _search_trials
            self._search_trials += 1  # #copilot change: C0103 - use snake_case
            return True  # next trial
        
        # If we get here, we've tried enough times without success
        logging.error(
            f"{dt.now().strftime('%c')}: Required number of MPPTs not found. Exiting."  # #copilot change: C0209, W1201 - using f-string
        )
        sys.exit()

    # #################################################################################
    # #################################################################################
    # ### aggregate values of physical batteries, perform calculations, update Dbus ###
    # #################################################################################
    # #################################################################################

    def _update(self):
        
        # DC
        voltage = 0
        current = 0
        power = 0

        # Capacity
        soc = 0
        capacity = 0
        installed_capacity = 0
        consumed_amphours = 0
        time_to_go = 0

        # Temperature
        temperature = 0
        max_cell_temp_list = []  # list, maxima of all physical batteries
        min_cell_temp_list = []  # list, minima of all physical batteries

        # Extras
        cell_voltages_dict = {}
        max_cell_voltage_dict = (
            {}
        )  # dictionary {'ID' : MaxCellVoltage, ... } for all physical batteries
        min_cell_voltage_dict = (
            {}
        )  # dictionary {'ID' : MinCellVoltage, ... } for all physical batteries
        nr_of_modules_online = 0
        nr_of_modules_offline = 0
        nr_of_modules_blocking_charge = 0
        nr_of_modules_blocking_discharge = 0
        voltages_sum_dict = {}  # battery voltages from sum of cells, Marvo2011
        charge_voltage_reduced_list = []

        # Alarms
        low_voltage_alarm_list = []  # lists to find maxima
        high_voltage_alarm_list = []
        low_cell_voltage_alarm_list = []
        low_soc_alarm_list = []
        high_charge_current_alarm_list = []
        high_discharge_current_alarm_list = []
        cell_imbalance_alarm_list = []
        internal_failure_alarm_list = []
        high_charge_temperature_alarm_list = []
        low_charge_temperature_alarm_list = []
        high_temperature_alarm_list = []
        low_temperature_alarm_list = []
        bms_cable_alarm_list = []

        # Charge/discharge parameters
        max_charge_current_list = (
            []
        )  # the minimum of MaxChargeCurrent * NR_OF_BATTERIES to be transmitted
        max_discharge_current_list = (
            []
        )  # the minimum of MaxDischargeCurrent * NR_OF_BATTERIES to be transmitted
        max_charge_voltage_list = (
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
            for i in self._batteries_dict:  # Marvo2011

                # DC
          vaConsumedAmphours"
                    )
                    Capacity += self._dbusMon.dbusmon.get_value step = "Read V, I, P"  # to detect error
       Capacity"
                    )
                    Soc += self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Soc"
                    ) * self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/InstalledCapacity"
                    )
                    ttg = self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/TimeToGo"
                    )
                    if (ttg is not None) and (TimeToGo is not None):
                        TimeToGo += ttg * self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/InstalledCapacity"
                        )
                    else:
                        TimeToGo = None

                # Temperature
                step = "Read temperatures"
                Temperature += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/Dc/0/Temperature"
                )
                MaxCellTemp_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MaxCellTemperature"
                    )
                )
                MinCellTemp_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MinCellTemperature"
                    )
                )

                # Cell voltages
                step = "Read max. and min cell voltages and voltage sum"  # cell ID : its voltage
                MaxCellVoltage_dict[
                    "%s_%s"
                    % (
                        i,
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/System/MaxVoltageCellId"
                        ),
                    )
                ] = self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/MaxCellVoltage"
                )
                MinCellVoltage_dict[
                    "%s_%s"
                    % (
                        i,
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/System/MinVoltageCellId"
                        ),
                    )
                ] = self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/MinCellVoltage"
                )

                 # here an exception is raised and new read trial initiated if None is on Dbus
                volt_sum_get = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], "/Voltages/Sum")
                if volt_sum_get != None:
                    VoltagesSum_dict[i] = volt_sum_get
                else:
                    raise TypeError(f"Battery {i} returns None value of /Voltages/Sum. Please check, if the setting 'BATTERY_CELL_DATA_FORMAT=1' in dbus-serialbattery config.")

                # Battery state
                step = "Read battery state"
                NrOfModulesOnline += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesOnline"
                )
                NrOfModulesOffline += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesOffline"
                )
                NrOfModulesBlockingCharge += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesBlockingCharge"
                )
                NrOfModulesBlockingDischarge += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesBlockingDischarge"
                )  # sum of modules blocking discharge

                step = "Read cell voltages"
                for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                    cellVoltages_dict["%s_Cell%d" % (i, j + 1)] = (
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Voltages/Cell%d" % (j + 1)
                        )
                    )

                # Alarms
                step = "Read alarms"
                LowVoltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowVoltage"
                    )
                )
                HighVoltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighVoltage"
                    )
                )
                LowCellVoltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowCellVoltage"
                    )
                )
                LowSoc_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowSoc"
                    )
                )
                HighChargeCurrent_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeCurrent"
                    )
                )
                HighDischargeCurrent_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighDischargeCurrent"
                    )
                )
                CellImbalance_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/CellImbalance"
                    )
                )
                InternalFailure_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/InternalFailure_alarm"
                    )
                )
                HighChargeTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeTemperature"
                    )
                )
                LowChargeTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowChargeTemperature"
                    )
                )
                HighTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighTemperature"
                    )
                )
                LowTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowTemperature"
                    )
                )
                BmsCable_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/BmsCable"
                    )
                )

                if (
                    settings.OWN_CHARGE_PARAMETERS
                ):  # calculate reduction of charge voltage as sum of overvoltages of all cells
                    step = "Calculate CVL reduction"
                    cellOvervoltage = 0
                    for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                        cellVoltage = self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Voltages/Cell%d" % (j + 1)
                        )
                        if cellVoltage > settings.MAX_CELL_VOLTAGE:
                            cellOvervoltage += cellVoltage - settings.MAX_CELL_VOLTAGE
                    chargeVoltageReduced_list.append(
                        VoltagesSum_dict[i] - cellOvervoltage
                    )

                else:  # Aggregate charge/discharge parameters
                    step = "Read charge parameters"
                    MaxChargeCurrent_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeCurrent"
                        )
                    )  # list of max. charge currents to find minimum
                    MaxDischargeCurrent_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxDischargeCurrent"
                        )
                    )  # list of max. discharge currents  to find minimum
                    MaxChargeVoltage_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeVoltage"
                        )
                    )  # list of max. charge voltages  to find minimum
                    ChargeMode_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/ChargeMode"
                        )
                    )  # list of charge modes of batteries (Bulk, Absorption, Float, Keep always max voltage)

                step = "Read Allow to"
                AllowToCharge_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToCharge"
                    )
                )  # list of AllowToCharge to find minimum
                AllowToDischarge_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToDischarge"
                    )
                )  # list of AllowToDischarge to find minimum
                AllowToBalance_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToBalance"
                    )
                )  # list of AllowToBalance to find minimum

            step = "Find max. and min. cell voltage of all batteries"
            # placed in try-except structure for the case if some values are of None.
            # The _max() and _min() don't work with dictionaries
            MaxVoltageCellId = max(MaxCellVoltage_dict, key=MaxCellVoltage_dict.get)
            MaxCellVoltage = MaxCellVoltage_dict[MaxVoltageCellId]
            MinVoltageCellId = min(MinCellVoltage_dict, key=MinCellVoltage_dict.get)
            MinCellVoltage = MinCellVoltage_dict[MinVoltageCellId]

        except Exception as err:
            self._readTrials += 1
            logging.error("%s: Error: %s." % ((dt.now()).strftime("%c"), err))
            logging.error("Occured during step %s, Battery %s." % (step, i))
            logging.error("Read trial nr. %d" % self._readTrials)
            if self._readTrials > settings.READ_TRIALS:
                logging.error(
                    "%s: DBus read failed. Exiting." % (dt.now()).strftime("%c")
                )
                sys.exit()
            else:
                return True  # next call allowed

        self._readTrials = 0  # must be reset after try-except

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
                        "%s: Victron current is None. Using BMS current and power instead."
                        % (dt.now()).strftime("%c")
                    )  # the BMS values are not overwritten

            except Exception:
                logging.error(
                    "%s: Victron current read error. Using BMS current and power instead."
                    % (dt.now()).strftime("%c")
                )  # the BMS values are not overwritten

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
                        "%s: CVL increase for balancing activated."
                        % (dt.now()).strftime("%c")
                    )

                if self._balancing == 1:
                    ChargeVoltageBattery = CVL_BALANCING
                    if (Voltage >= CVL_BALANCING) and (
                        (MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX
                    ):
                        self._balancing = 2
                        logging.info(
                            "%s: Balancing goal reached." % (dt.now()).strftime("%c")
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
                            "%s: CVL increase for balancing de-activated."
                            % (dt.now()).strftime("%c")
                        )

                if self._balancing == 0:
                    ChargeVoltageBattery = CVL_NORMAL

            elif (
                (time_unbalanced > 0)
                and (Voltage >= CVL_BALANCING)
                and ((MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX)
            ):  # if normal charging voltage is 100% SoC and balancing is finished
                logging.info(
                    "%s: Balancing goal reached with full charging set as normal. Updating last_balancing file."
                    % (dt.now()).strftime("%c")
                )
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
                    self._dynamicCVL = True
                    logging.info(
                        "%s: Dynamic CVL reduction started." % (dt.now()).strftime("%c")
                    )
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
                    "%s: DC-coupled PV feed-in de-activated."
                    % (dt.now()).strftime("%c")
                )
                MaxChargeVoltage = min(
                    (min(chargeVoltageReduced_list)), ChargeVoltageBattery
                )  # avoid exceeding MAX_CELL_VOLTAGE

            else:
                MaxChargeVoltage = ChargeVoltageBattery

                if self._dynamicCVL:
                    self._dynamicCVL = False
                    logging.info(
                        "%s: Dynamic CVL reduction finished."
                        % (dt.now()).strftime("%c")
                    )

                if (
                    (MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX
                ) and self._DCfeedActive:  # re-enable DC-feed if it was enabled before
                    self._dbusMon.dbusmon.set_value(
                        "com.victronenergy.settings",
                        "/Settings/CGwacs/OvervoltageFeedIn",
                        1,
                    )  # enable DC-coupled PV feed-in
                    logging.info(
                        "%s: DC-coupled PV feed-in re-activated."
                        % (dt.now()).strftime("%c")
                    )
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
            bus["/Dc/0/Voltage"] = voltage
            bus["/Dc/0/Current"] = current
            bus["/Dc/0/Power"] = power

            # send charge
            bus["/Soc"] = soc
            bus["/TimeToGo"] = time_to_go
            bus["/Capacity"] = capacity
            bus["/InstalledCapacity"] = installed_capacity
            bus["/ConsumedAmphours"] = consumed_amphours

            # send temperature
            bus["/Dc/0/Temperature"] = temperature
            bus["/System/MaxCellTemperature"] = max_cell_temp
            bus["/System/MinCellTemperature"] = min_cell_temp

            # send cell voltages
            bus["/System/MaxCellVoltage"] = max_cell_voltage
            bus["/System/MaxVoltageCellId"] = max_voltage_cell_id
            bus["/System/MinCellVoltage"] = min_cell_voltage
            bus["/System/MinVoltageCellId"] = min_voltage_cell_id
            bus["/Voltages/Sum"] = voltages_sum
            bus["/Voltages/Diff"] = round(
                max_cell_voltage - min_cell_voltage, 3
            )  # Marvo2011

            if settings.SEND_CELL_VOLTAGES == 1:  # Marvo2011
                for cell_id, current_cell in enumerate(cell_voltages_dict):
                    bus[
                        "/Voltages/%s" % (re.sub("[^A-Za-z0-9_]+", "", current_cell))
                    ] = cell_voltages_dict[current_cell]

            # send battery state
            bus["/System/NrOfCellsPerBattery"] = settings.NR_OF_CELLS_PER_BATTERY
            bus["/System/NrOfModulesOnline"] = nr_of_modules_online
            bus["/System/NrOfModulesOffline"] = nr_of_modules_offline
            bus["/System/NrOfModulesBlockingCharge"] = nr_of_modules_blocking_charge
            bus["/System/NrOfModulesBlockingDischarge"] = nr_of_modules_blocking_discharge

            # send alarms
            bus["/Alarms/LowVoltage"] = low_voltage_alarm
            bus["/Alarms/HighVoltage"] = high_voltage_alarm
            bus["/Alarms/LowCellVoltage"] = low_cell_voltage_alarm
            bus["/Alarms/LowSoc"] = low_soc_alarm
            bus["/Alarms/HighChargeCurrent"] = high_charge_current_alarm
            bus["/Alarms/HighDischargeCurrent"] = high_discharge_current_alarm
            bus["/Alarms/CellImbalance"] = cell_imbalance_alarm
            bus["/Alarms/InternalFailure"] = internal_failure_alarm
            bus["/Alarms/HighChargeTemperature"] = high_charge_temperature_alarm
            bus["/Alarms/LowChargeTemperature"] = low_charge_temperature_alarm
            bus["/Alarms/HighTemperature"] = high_temperature_alarm
            bus["/Alarms/LowTemperature"] = low_temperature_alarm
            bus["/Alarms/BmsCable"] = bms_cable_alarm

            # send charge/discharge control
            bus["/Info/MaxChargeCurrent"] = MaxChargeCurrent
            bus["/Info/MaxChargeCurrent"] =  = Mhargecurrent
            bus["/Info/MaxDischargeCurrent"] = Charischargeeurrent
            bus["/Info/MaxChargeVoltage"] = 
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
            if self._logTimer < settings.LOG_PERIOD:
                self._logTimer += 1
            else:
                self._logTimer = 0
                logging.info("%s: Repetitive logging:" % dt.now().strftime("%c"))
                logging.info(
                    "  CVL: %.1fV, CCL: %.0fA, DCL: %.0fA"
                    % (MaxChargeVoltage, MaxChargeCurrent, MaxDischargeCurrent)
                )
                logging.info(
                    "  Bat. voltage: %.1fV, Bat. current: %.0fA, SoC: %.1f%%, Balancing state: %d"
                    % (Voltage, Current, Soc, self._balancing)
                )
                logging.info(
                    "  Min. cell voltage: %s: %.3fV, Max. cell voltage: %s: %.3fV, difference: %.3fV"
                    % (
                        MinVoltageCellId,
                        MinCellVoltage,
                        MaxVoltageCellId,
                        MaxCellVoltage,
                        MaxCellVoltage - MinCellVoltage,
                    )
                )

        return True


# ################
# ################
# ## Main loop ###
# ################
# ################


def main():

    logging.basicConfig(level=logging.INFO)
    logging.info("%s: Starting AggregateBatteries." % (dt.now()).strftime("%c"))
    from dbus.mainloop.glib import DBusGMainLoop

    DBusGMainLoop(set_as_default=True)

    DbusAggBatService()

    logging.info(
        "%s: Connected to DBus, and switching over to GLib.MainLoop()"
        % (dt.now()).strftime("%c")
    )
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
atteries_dict[i], "/Dc/0/Power"
ower                    self._batteries_dict[i], "/Dc/0/Currestep = "Read and calculate capacity, SoC, Time to go"
                InstalledCapacity += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/InstalledCapacity"
nt                  self._batterva
                if not settings.OWN_SOC:
                    ConsumedAmphours += self._dbusMon.dbusmon.get_valueict[i], "/Dc       Voltage += self._dbusMon.dbusmon.get_vaConsumedAmphours"
                    )
                    Capacity += self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Capacity"
                    )
                    Soc += self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Soc"
                    ) * self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/InstalledCapacity"
                    )
                    ttg = self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/TimeToGo"
                    )
                    if (ttg is not None) and (TimeToGo is not None):
                        TimeToGo += ttg * self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/InstalledCapacity"
                        )
                    else:
                        TimeToGo = None

                # Temperature
                step = "Read temperatures"
                Temperature += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/Dc/0/Temperature"
                )
                MaxCellTemp_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MaxCellTemperature"
                    )
                )
                MinCellTemp_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MinCellTemperature"
                    )
                )

                # Cell voltages
                step = "Read max. and min cell voltages and voltage sum"  # cell ID : its voltage
                MaxCellVoltage_dict[
                    "%s_%s"
                    % (
                        i,
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/System/MaxVoltageCellId"
                        ),
                    )
                ] = self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/MaxCellVoltage"
                )
                MinCellVoltage_dict[
                    "%s_%s"
                    % (
                        i,
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/System/MinVoltageCellId"
                        ),
                    )
                ] = self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/MinCellVoltage"
                )

                 # here an exception is raised and new read trial initiated if None is on Dbus
                volt_sum_get = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], "/Voltages/Sum")
                if volt_sum_get != None:
                    VoltagesSum_dict[i] = volt_sum_get
                else:
                    raise TypeError(f"Battery {i} returns None value of /Voltages/Sum. Please check, if the setting 'BATTERY_CELL_DATA_FORMAT=1' in dbus-serialbattery config.")

                # Battery state
                step = "Read battery state"
                NrOfModulesOnline += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesOnline"
                )
                NrOfModulesOffline += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesOffline"
                )
                NrOfModulesBlockingCharge += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesBlockingCharge"
                )
                NrOfModulesBlockingDischarge += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesBlockingDischarge"
                )  # sum of modules blocking discharge

                step = "Read cell voltages"
                for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                    cellVoltages_dict["%s_Cell%d" % (i, j + 1)] = (
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Voltages/Cell%d" % (j + 1)
                        )
                    )

                # Alarms
                step = "Read alarms"
                LowVoltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowVoltage"
                    )
                )
                HighVoltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighVoltage"
                    )
                )
                LowCellVoltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowCellVoltage"
                    )
                )
                LowSoc_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowSoc"
                    )
                )
                HighChargeCurrent_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeCurrent"
                    )
                )
                HighDischargeCurrent_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighDischargeCurrent"
                    )
                )
                CellImbalance_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/CellImbalance"
                    )
                )
                InternalFailure_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/InternalFailure_alarm"
                    )
                )
                HighChargeTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeTemperature"
                    )
                )
                LowChargeTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowChargeTemperature"
                    )
                )
                HighTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighTemperature"
                    )
                )
                LowTemperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowTemperature"
                    )
                )
                BmsCable_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/BmsCable"
                    )
                )

                if (
                    settings.OWN_CHARGE_PARAMETERS
                ):  # calculate reduction of charge voltage as sum of overvoltages of all cells
                    step = "Calculate CVL reduction"
                    cellOvervoltage = 0
                    for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                        cellVoltage = self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Voltages/Cell%d" % (j + 1)
                        )
                        if cellVoltage > settings.MAX_CELL_VOLTAGE:
                            cellOvervoltage += cellVoltage - settings.MAX_CELL_VOLTAGE
                    chargeVoltageReduced_list.append(
                        VoltagesSum_dict[i] - cellOvervoltage
                    )

                else:  # Aggregate charge/discharge parameters
                    step = "Read charge parameters"
                    MaxChargeCurrent_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeCurrent"
                        )
                    )  # list of max. charge currents to find minimum
                    MaxDischargeCurrent_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxDischargeCurrent"
                        )
                    )  # list of max. discharge currents  to find minimum
                    MaxChargeVoltage_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeVoltage"
                        )
                    )  # list of max. charge voltages  to find minimum
                    ChargeMode_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/ChargeMode"
                        )
                    )  # list of charge modes of batteries (Bulk, Absorption, Float, Keep always max voltage)

                step = "Read Allow to"
                AllowToCharge_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToCharge"
                    )
                )  # list of AllowToCharge to find minimum
                AllowToDischarge_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToDischarge"
                    )
                )  # list of AllowToDischarge to find minimum
                AllowToBalance_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToBalance"
                    )
                )  # list of AllowToBalance to find minimum

            step = "Find max. and min. cell voltage of all batteries"
            # placed in try-except structure for the case if some values are of None.
            # The _max() and _min() don't work with dictionaries
            MaxVoltageCellId = max(MaxCellVoltage_dict, key=MaxCellVoltage_dict.get)
            MaxCellVoltage = MaxCellVoltage_dict[MaxVoltageCellId]
            MinVoltageCellId = min(MinCellVoltage_dict, key=MinCellVoltage_dict.get)
            MinCellVoltage = MinCellVoltage_dict[MinVoltageCellId]

        except Exception as err:
            self._readTrials += 1
            logging.error("%s: Error: %s." % ((dt.now()).strftime("%c"), err))
            logging.error("Occured during step %s, Battery %s." % (step, i))
            logging.error("Read trial nr. %d" % self._readTrials)
            if self._readTrials > settings.READ_TRIALS:
                logging.error(
                    "%s: DBus read failed. Exiting." % (dt.now()).strftime("%c")
                )
                sys.exit()
            else:
                return True  # next call allowed

        self._readTrials = 0  # must be reset after try-except

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
                        "%s: Victron current is None. Using BMS current and power instead."
                        % (dt.now()).strftime("%c")
                    )  # the BMS values are not overwritten

            except Exception:
                logging.error(
                    "%s: Victron current read error. Using BMS current and power instead."
                    % (dt.now()).strftime("%c")
                )  # the BMS values are not overwritten

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
                        "%s: CVL increase for balancing activated."
                        % (dt.now()).strftime("%c")
                    )

                if self._balancing == 1:
                    ChargeVoltageBattery = CVL_BALANCING
                    if (Voltage >= CVL_BALANCING) and (
                        (MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX
                    ):
                        self._balancing = 2
                        logging.info(
                            "%s: Balancing goal reached." % (dt.now()).strftime("%c")
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
                            "%s: CVL increase for balancing de-activated."
                            % (dt.now()).strftime("%c")
                        )

                if self._balancing == 0:
                    ChargeVoltageBattery = CVL_NORMAL

            elif (
                (time_unbalanced > 0)
                and (Voltage >= CVL_BALANCING)
                and ((MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX)
            ):  # if normal charging voltage is 100% SoC and balancing is finished
                logging.info(
                    "%s: Balancing goal reached with full charging set as normal. Updating last_balancing file."
                    % (dt.now()).strftime("%c")
                )
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
                    self._dynamicCVL = True
                    logging.info(
                        "%s: Dynamic CVL reduction started." % (dt.now()).strftime("%c")
                    )
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
                    "%s: DC-coupled PV feed-in de-activated."
                    % (dt.now()).strftime("%c")
                )
                MaxChargeVoltage = min(
                    (min(chargeVoltageReduced_list)), ChargeVoltageBattery
                )  # avoid exceeding MAX_CELL_VOLTAGE

            else:
                MaxChargeVoltage = ChargeVoltageBattery

                if self._dynamicCVL:
                    self._dynamicCVL = False
                    logging.info(
                        "%s: Dynamic CVL reduction finished."
                        % (dt.now()).strftime("%c")
                    )

                if (
                    (MaxCellVoltage - MinCellVoltage) < settings.CELL_DIFF_MAX
                ) and self._DCfeedActive:  # re-enable DC-feed if it was enabled before
                    self._dbusMon.dbusmon.set_value(
                        "com.victronenergy.settings",
                        "/Settings/CGwacs/OvervoltageFeedIn",
                        1,
                    )  # enable DC-coupled PV feed-in
                    logging.info(
                        "%s: DC-coupled PV feed-in re-activated."
                        % (dt.now()).strftime("%c")
                    )
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
                logging.error('%s: BMS connection lost.' % (dt.now()).strftime('%c'))
            """

            # this does not control the charger, is only displayed in GUI
            bus["/Io/AllowToCharge"] = allow_to_charge
            bus["/Io/AllowToDischarge"] = allow_to_discharge
            bus["/Io/AllowToBalance"] = allow_to_balance

        return True


# ################
# ################
# ## Main loop ###
# ################
# ################


def main():

    logging.basicConfig(level=logging.INFO)
    logging.info("%s: Starting AggregateBatteries." % (dt.now()).strftime("%c"))
    from dbus.mainloop.glib import DBusGMainLoop

    DBusGMainLoop(set_as_default=True)

    DbusAggBatService()

    logging.info(
        "%s: Connected to DBus, and switching over to GLib.MainLoop()"
        % (dt.now()).strftime("%c")
    )
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
