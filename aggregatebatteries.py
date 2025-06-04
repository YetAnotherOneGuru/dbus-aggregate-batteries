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
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


class SessionBus(dbus.bus.BusConnection):
    def __new__(cls):
        # pylint: disable=protected-access  # copilot change: W0212
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)  # copilot change: W0212

def get_bus() -> dbus.bus.BusConnection:
    return SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else SystemBus()

class DbusAggBatService(object):
    def __init__(self, servicename="com.victronenergy.battery.aggregate"):
        """Initialize the DbusAggBatService object."""  # copilot change: C0116
        self._fn = Functions()
        self._batteries_dict = {}  # marvo2011
        self._multi = None
        self._mppts_list = []
        self._smartShunt = None  # copilot change: C0103
        self._search_trials = 0  # copilot change: C0103
        self._read_trials = 0  # copilot change: C0103
        self._max_charge_voltage_old = 0  # copilot change: C0103
        self._max_charge_current_old = 0  # copilot change: C0103
        self._max_discharge_current_old = 0  # copilot change: C0103
        # implementing hysteresis for allowing discharge
        self._fully_discharged = False  # copilot change: C0103
        self._dbus_conn = get_bus()  # copilot change: C0103
        logging.info("### Initialise VeDbusService") # copilot change: W1201
        self._dbusservice = VeDbusService(servicename, self._dbusConn, register=False)
        logging.info("#### Done: Init of VeDbusService")  # copilot change: W1201
        self._time_old = tt.time()  # copilot change: C0103
        # written when dynamic CVL limit activated
        self._dc_feed_active = False  # copilot change: C0103
        # 0: inactive; 1: goal reached, waiting for discharging under nominal voltage; 2: nominal voltage reached
        self._balancing = 0
        # Day in year
        self._last_balancing = 0  # copilot change: C0103
        # set if the CVL needs to be reduced due to peaking
        self._dynamic_cvl = False  # copilot change: C0103
        # measure logging period in seconds
        self._log_timer = 0  # copilot change: C0103

        # read initial charge from text file
        try:
            self._charge_file = open(
                "/data/dbus-aggregate-batteries/charge", "r"
            )  # read
            self._own_charge = float(self._charge_file.readline().strip())  # copilot change: C0103
            self._charge_file.close()
            self._own_charge_old = self._own_charge  # copilot change: C0103
            logging.info(
                f"{(dt.now()).strftime('%c')}: Initial Ah read from file: {self._own_charge:.0f}Ah"  # copilot change: C0209

            )
        except Exception as exc:  # copilot change: W0718
            logging.error(
                f"{(dt.now()).strftime('%c')}: Charge file read error. Exiting. Exception: {exc}"  # copilot change: W0718, C0209
            )
            sys.exit()

        if (
            settings.OWN_CHARGE_PARAMETERS
        ):  # read the day of the last balancing from text file
            try:
                self._last_balancing_file = open(
                    "/data/dbus-aggregate-batteries/last_balancing", "r"
                )  # read
                self._last_balancing = int(self._last_balancing_file.readline().strip())  # copilot change: C0103
                self._last_balancing_file.close()  # copilot change: C0103
                time_unbalanced = (
                    int((dt.now()).strftime("%j")) - self._last_balancing  # copilot change: C0103
                )  # in days
                if time_unbalanced < 0:
                    time_unbalanced += 365  # year change
                logging.info(
                    "Last balancing done at the %d. day of the year", self._last_balancing  # copilot change: W1201

                )
                logging.info(
                    "Batteries balanced %d days ago.", time_unbalanced  # copilot change: W1201
                )

            except Exception as exc:  # copilot change: W0718
                logging.error(
                    f"{(dt.now()).strftime('%c')}: Last balancing file read error. Exiting. Exception: {exc}"  # copilot change: W0718


                )
                sys.exit()

        # Create the management objects, as specified in the ccgx dbus-api document
        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", f"Python {platform.python_version()}")  # copilot change: C0209
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
            gettextcallback=lambda a, x: f"{x:.2f}V",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Dc/0/Current",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.2f}A",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Dc/0/Power",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.0f}W",  # copilot change: C0209
        )

        # Create capacity paths
        self._dbusservice.add_path("/Soc", None, writeable=True)
        self._dbusservice.add_path(
            "/Capacity",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.0f}Ah",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/InstalledCapacity",
            None,
            gettextcallback=lambda a, x: f"{x:.0f}Ah",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/ConsumedAmphours", None, gettextcallback=lambda a, x: f"{x:.0f}Ah"  # copilot change: C0209
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
            gettextcallback=lambda a, x: f"{x:.3f}V",  # copilot change: C0209
        )  # marvo2011
        self._dbusservice.add_path("/System/MinVoltageCellId", None, writeable=True)
        self._dbusservice.add_path(
            "/System/MaxCellVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.3f}V",  # copilot change: C0209
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
            gettextcallback=lambda a, x: f"{x:.3f}V",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Voltages/Diff",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.3f}V",  # copilot change: C0209
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
            gettextcallback=lambda a, x: f"{x:.1f}A",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Info/MaxDischargeCurrent",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.1f}A",  # copilot change: C0209
        )
        self._dbusservice.add_path(
            "/Info/MaxChargeVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: f"{x:.2f}V",  # copilot change: C0209
        )
        self._dbusservice.add_path("/Io/AllowToCharge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToDischarge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToBalance", None, writeable=True)

        # register VeDbusService after all paths where added
        logging.info("Registering VeDbusService")  # copilot change: W1201
        self._dbusservice.register()
        
        x = Thread(target=self._startMonitor)  # copilot change: C0303
        x.start()

        GLib.timeout_add(1000, self._find_settings)  # search com.victronenergy.settings

    # #############################################################################################################
    # #############################################################################################################
    # ## Starting battery dbus monitor in external thread (otherwise collision with AggregateBatteries service) ###
    # #############################################################################################################
    # #############################################################################################################
    def _startMonitor(self):
        """Start the battery dbus monitor in an external thread."""  # copilot change: C0116
        logging.info("%s: Starting battery monitor.", dt.now().strftime('%c'))  # copilot change: W1201
        self._dbusMon = DbusMon()

    # ####################################################################
    # ####################################################################
    # ## search Settings, to maintain CCL during dynamic CVL reduction ###
    # https://www.victronenergy.com/upload/documents/Cerbo_GX/140558-CCGX__Venus_GX__Cerbo_GX__Cerbo-S_GX_Manual-pdf-en.pdf, P72  # noqa: E501
    # ####################################################################
    # ####################################################################

    def _find_settings(self):
        """Search Settings to maintain CCL during dynamic CVL reduction."""  # copilot change: C0116
        logging.info("%s: Searching Settings: Trial Nr. %d", dt.now().strftime('%c'), self._searchTrials + 1)  # copilot change: W1201

        try:
            for service in self._dbusConn.list_names():
                if "com.victronenergy.settings" in service:
                    self._settings = service
                    logging.info("%s: com.victronenergy.settings found.", dt.now().strftime('%c'))  # copilot change: W1201)

        except Exception as exc:  # copilot change: W0718
            logging.error(f"{(dt.now()).strftime('%c')}: Exception in _find_settings: {exc}")  # copilot change: W0718

        if self._settings is not None:
            self._searchTrials = 0
            GLib.timeout_add(
                5000, self._find_batteries
            )  # search batteries on DBus if present
            return False  # all OK, stop calling this function
        elif self._searchTrials < settings.SEARCH_TRIALS:
            self._searchTrials += 1
            return True  # next trial
        else:
            logging.error(f"{dt.now().strftime('%c')}: com.victronenergy.settings not found. Exiting.")  # copilot change: C0303

            sys.exit()

    # ####################################################################
    # ####################################################################
    # ## search physical batteries and optional SmartShunt on DC loads ###
    # ####################################################################
    # ####################################################################

    def _find_batteries(self):
        """Search physical batteries and optional SmartShunt on DC loads."""  # copilot change: C0116
        self._batteries_dict = {}  # Marvo2011
        batteries_count = 0  # copilot change: C0103
        product_name = ""  # copilot change: C0103
        logging.info(
            "%s: Searching batteries: Trial Nr. %d", dt.now().strftime('%c'), self._searchTrials + 1  # copilot change: W1201
        )



        try:  # copilot change: W0718
            for service in self._dbusConn.list_names():
                if "com.victronenergy" in service:
                    logging.info(
                        "%s: Dbusmonitor sees: %s", dt.now().strftime('%c'), service  # copilot change: W1201
                    )
                if settings.BATTERY_SERVICE_NAME in service:
                    product_name = self._dbusMon.dbusmon.get_value(
                        service, settings.BATTERY_PRODUCT_NAME_PATH
                    )  # copilot change: C0103
                    if (product_name is not None) and (settings.BATTERY_SERVICE_NAME in product_name):  # copilot change: C0103
                        logging.info(
                            "%s: Correct battery product name %s found in the service %s", dt.now().strftime('%c'), product_name, service  # copilot change: W1201
                        )
                        # Custom name, if exists, Marvo2011
                        try:
                            battery_name = self._dbusMon.dbusmon.get_value(
                                service, settings.BATTERY_INSTANCE_NAME_PATH
                            )  # copilot change: C0103
                        except Exception as exc:  # copilot change: W0718
                            battery_name = f"Battery{batteries_count + 1}"  # copilot change: C0103
                            logging.error(f"{(dt.now()).strftime('%c')}: Exception in battery name fetch: {exc}")  # copilot change: W0718
                        # Check if all batteries have custom names
                        if battery_name in self._batteries_dict:
                            battery_name = f"{battery_name}{batteries_count + 1}"  # copilot change: C0103

                        self._batteries_dict[battery_name] = service  # copilot change: C0103
                        logging.info(
                            "%s: %s, named as: %s.", dt.now().strftime('%c'), self._dbusMon.dbusmon.get_value(service, '/ProductName'), battery_name  # copilot change: W1201

                        )

                        batteries_count += 1  # copilot change: C0103

                        # Create voltage paths with battery names
                        if settings.SEND_CELL_VOLTAGES == 1:
                            for cellId in range(
                                1, (settings.NR_OF_CELLS_PER_BATTERY) + 1
                            ):
                                self._dbusservice.add_path(
                                    "/Voltages/%s_Cell%d"
                                    % (
                                        re.sub("[^A-Za-z0-9_]+", "", battery_name),
                                        cellId,
                                    ),
                                    None,
                                    writeable=True,
                                    gettextcallback=lambda a, x: f"{x:.3f}V",  # copilot change: C0209
                                )

                        
                        # Check if Nr. of cells is equal  # copilot change: C0303
                        if (
                            self._dbusMon.dbusmon.get_value(
                                service, "/System/NrOfCellsPerBattery"
                            )
                            != settings.NR_OF_CELLS_PER_BATTERY
                        ):
                            logging.error(f"{(dt.now()).strftime('%c')}: Number of cells of batteries is not correct. Exiting.")  # copilot change: C0209



                            sys.exit()

                        # end of section, Marvo2011

                    elif (
                        (product_name is not None) and (settings.SMARTSHUNT_NAME_KEY_WORD in product_name)
                    ):  # if SmartShunt found, can be used for DC load current
                        self._smartShunt = service
                        logging.info(
                            "%s: Correct Smart Shunt product name %s found in the service %s", dt.now().strftime('%c'), product_name, service  # copilot change: W1201
                        )

        except Exception as exc:  # copilot change: W0718
            logging.error(f"{(dt.now()).strftime('%c')}: Exception in _find_batteries: {exc}")  # copilot change: W0718
        logging.info(
            "%s: %d batteries found.", dt.now().strftime('%c'), batteries_count  # copilot change: W1201
        )

        if batteries_count == settings.NR_OF_BATTERIES:  # copilot change: C0103
            if settings.CURRENT_FROM_VICTRON:
                self._searchTrials = 0
                GLib.timeout_add(
                    1000, self._find_multis
                )  # if current from Victron stuff search multi/quattro on DBus
            else:
                self._timeOld = tt.time()
                GLib.timeout_add(
                    1000, self._update
                )  # if current from BMS start the _update loop
            return False  # all OK, stop calling this function
        elif self._searchTrials < settings.SEARCH_TRIALS:
            self._searchTrials += 1
            return True  # next trial
        else:
            logging.error(f"{(dt.now()).strftime('%c')}: Required number of batteries not found. Exiting."  # copilot change: C0209



            )
            sys.exit()

    # #########################################################################
    # #########################################################################
    # ## search Multis or Quattros (if selected for DC current measurement) ###
    # #########################################################################
    # #########################################################################

    def _find_multis(self):
        """Search Multis or Quattros (if selected for DC current measurement)."""  # copilot change: C0116
        logging.info(
            "%s: Searching Multi/Quatro VEbus: Trial Nr. %d", dt.now().strftime('%c'), self._searchTrials + 1  # copilot change: W1201
        )



        try:
            for service in self._dbusConn.list_names():
                if settings.MULTI_KEY_WORD in service:
                    self._multi = service
                    logging.info(
                        "%s: %s found.", dt.now().strftime('%c'), self._dbusMon.dbusmon.get_value(service, '/ProductName')  # copilot change: W1201
                    )
        except Exception as exc:  # copilot change: W0718
            logging.error(f"{(dt.now()).strftime('%c')}: Exception in _find_multis: {exc}")  # copilot change: W0718




        if self._multi is not None:
            if settings.NR_OF_MPPTS > 0:
                self._searchTrials = 0
                GLib.timeout_add(
                    1000, self._find_mppts
                )  # search MPPTs on DBus if present
            else:
                self._timeOld = tt.time()
                GLib.timeout_add(
                    1000, self._update
                )  # if no MPPTs start the _update loop
            return False  # all OK, stop calling this function
        elif self._searchTrials < settings.SEARCH_TRIALS:
            self._searchTrials += 1
            return True  # next trial
        else:
            logging.error(
                f"{(dt.now()).strftime('%c')}: Multi/Quattro not found. Exiting."  # copilot change: C0209
            )
            sys.exit()

    # ############################################################
    # ############################################################
    # ## search MPPTs (if selected for DC current measurement) ###
    # ############################################################
    # ############################################################

    def _find_mppts(self):
        """Search MPPTs (if selected for DC current measurement)."""  # copilot change: C0116
        self._mppts_list = []
        mpptsCount = 0
        logging.info(
            "%s: Searching MPPTs: Trial Nr. %d", dt.now().strftime('%c'), self._searchTrials + 1  # copilot change: W1201
        )



        
        
        
        try:
            for service in self._dbusConn.list_names():
                if settings.MPPT_KEY_WORD in service:
                    self._mppts_list.append(service)
                    logging.info(
                        "%s: %s found.", dt.now().strftime('%c'), self._dbusMon.dbusmon.get_value(service, '/ProductName')  # copilot change: W1201




                    )
                    mpptsCount += 1
        except Exception as exc:  # copilot change: W0718
            logging.error(f"{(dt.now()).strftime('%c')}: Exception in _find_mppts: {exc}")  # copilot change: W0718
        logging.info(
            "%s: %d MPPT(s) found.", dt.now().strftime('%c'), mpptsCount  # copilot change: W1201
        )
        if mpptsCount == settings.NR_OF_MPPTS:
            self._timeOld = tt.time()
            GLib.timeout_add(1000, self._update)
            return False  # all OK, stop calling this function
        elif self._searchTrials < settings.SEARCH_TRIALS:
            self._searchTrials += 1
            return True  # next trial
        else:
            logging.error(
                f"{(dt.now()).strftime('%c')}: Required number of MPPTs not found. Exiting."  # copilot change: C0209

            
            )
            sys.exit()

    # #################################################################################
    # #################################################################################
    # ### aggregate values of physical batteries, perform calculations, update Dbus ###
    # #################################################################################
    # #################################################################################

    def _update(self):
        """Aggregate values of physical batteries, perform calculations, update Dbus."""  # copilot change: C0116

        # DC
        voltage = 0  # copilot change: C0103
        current = 0  # copilot change: C0103
        power = 0  # copilot change: C0103

        # Capacity
        soc = 0  # copilot change: C0103
        capacity = 0  # copilot change: C0103
        installed_capacity = 0  # copilot change: C0103
        consumed_amphours = 0  # copilot change: C0103
        time_to_go = 0  # copilot change: C0103

        # Temperature
        temperature = 0  # copilot change: C0103
        max_cell_temp_list = []  # copilot change: C0103
        min_cell_temp_list = []  # copilot change: C0103

        # Extras
        cell_voltages_dict = {}  # copilot change: C0103
        max_cell_voltage_dict = (
            {}
        )  # dictionary {'ID' : MaxCellVoltage, ... } for all physical batteries
        min_cell_voltage_dict = (
            {}
        )  # dictionary {'ID' : MinCellVoltage, ... } for all physical batteries
        nr_of_modules_online = 0  # copilot change: C0103
        nr_of_modules_offline = 0  # copilot change: C0103
        nr_of_modules_blocking_charge = 0  # copilot change: C0103
        nr_of_modules_blocking_discharge = 0  # copilot change: C0103
        voltages_sum_dict = {}  # copilot change: C0103
        charge_voltage_reduced_list = []  # copilot change: C0103

        # Alarms
        low_voltage_alarm_list = []  # copilot change: C0103
        high_voltage_alarm_list = []  # copilot change: C0103
        low_cell_voltage_alarm_list = []  # copilot change: C0103
        low_soc_alarm_list = []  # copilot change: C0103
        high_charge_current_alarm_list = []  # copilot change: C0103
        high_discharge_current_alarm_list = []  # copilot change: C0103
        cell_imbalance_alarm_list = []  # copilot change: C0103
        internal_failure_alarm_list = []  # copilot change: C0103
        high_charge_temperature_alarm_list = []  # copilot change: C0103
        low_charge_temperature_alarm_list = []  # copilot change: C0103
        high_temperature_alarm_list = []  # copilot change: C0103
        low_temperature_alarm_list = []  # copilot change: C0103
        bms_cable_alarm_list = []  # copilot change: C0103

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
        allow_to_charge_list = []  # copilot change: C0103
        allow_to_discharge_list = []  # copilot change: C0103
        allow_to_balance_list = []  # copilot change: C0103
        charge_mode_list = []  # copilot change: C0103

        ####################################################
        # Get DBus values from all SerialBattery instances #
        ####################################################

        try:
            for i in self._batteries_dict:  # Marvo2011

                # DC
                step = "Read V, I, P"  # to detect error
                voltage += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/Dc/0/Voltage"
                )
                current += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/Dc/0/Current"
                )
                power += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/Dc/0/Power"
                )

                # Capacity
                step = "Read and calculate capacity, SoC, Time to go"
                installed_capacity += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/InstalledCapacity"
                )

                if not settings.OWN_SOC:
                    consumed_amphours += self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/ConsumedAmphours"
                    )
                    capacity += self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Capacity"
                    )
                    soc += self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Soc"
                    ) * self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/InstalledCapacity"
                    )
                    ttg = self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/TimeToGo"
                    )
                    if (ttg is not None) and (time_to_go is not None):
                        time_to_go += ttg * self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/InstalledCapacity"
                        )
                    else:
                        time_to_go = None

                # Temperature
                step = "Read temperatures"
                temperature += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/Dc/0/Temperature"
                )
                max_cell_temp_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MaxCellTemperature"
                    )
                )
                min_cell_temp_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/System/MinCellTemperature"
                    )
                )

                # Cell voltages
                step = "Read max. and min cell voltages and voltage sum"  # cell ID : its voltage
                max_cell_voltage_dict[
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
                min_cell_voltage_dict[
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
                    voltages_sum_dict[i] = volt_sum_get
                else:
                    raise TypeError(f"Battery {i} returns None value of /Voltages/Sum. Please check, if the setting 'BATTERY_CELL_DATA_FORMAT=1' in dbus-serialbattery config.")

                # Battery state
                step = "Read battery state"
                nr_of_modules_online += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesOnline"
                )
                nr_of_modules_offline += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesOffline"
                )
                nr_of_modules_blocking_charge += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesBlockingCharge"
                )
                nr_of_modules_blocking_discharge += self._dbusMon.dbusmon.get_value(
                    self._batteries_dict[i], "/System/NrOfModulesBlockingDischarge"
                )  # sum of modules blocking discharge

                step = "Read cell voltages"
                for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                    cell_voltages_dict["%s_Cell%d" % (i, j + 1)] = (
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Voltages/Cell%d" % (j + 1)
                        )
                    )

                # Alarms
                step = "Read alarms"
                low_voltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowVoltage"
                    )
                )
                high_voltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighVoltage"
                    )
                )
                low_cell_voltage_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowCellVoltage"
                    )
                )
                low_soc_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowSoc"
                    )
                )
                high_charge_current_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeCurrent"
                    )
                )
                high_discharge_current_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighDischargeCurrent"
                    )
                )
                cell_imbalance_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/CellImbalance"
                    )
                )
                internal_failure_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/InternalFailure_alarm"
                    )
                )
                high_charge_temperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighChargeTemperature"
                    )
                )
                low_charge_temperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowChargeTemperature"
                    )
                )
                high_temperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/HighTemperature"
                    )
                )
                low_temperature_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/LowTemperature"
                    )
                )
                bms_cable_alarm_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Alarms/BmsCable"
                    )
                )

                if (
                    settings.OWN_CHARGE_PARAMETERS
                ):  # calculate reduction of charge voltage as sum of overvoltages of all cells
                    step = "Calculate CVL reduction"
                    cell_overvoltage = 0
                    for j in range(settings.NR_OF_CELLS_PER_BATTERY):  # Marvo2011
                        cell_voltage = self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Voltages/Cell%d" % (j + 1)
                        )
                        if cell_voltage > settings.MAX_CELL_VOLTAGE:
                            cell_overvoltage += cell_voltage - settings.MAX_CELL_VOLTAGE
                    charge_voltage_reduced_list.append(
                        voltages_sum_dict[i] - cell_overvoltage
                    )

                else:  # Aggregate charge/discharge parameters
                    step = "Read charge parameters"
                    max_charge_current_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeCurrent"
                        )
                    )  # list of max. charge currents to find minimum
                    max_discharge_current_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxDischargeCurrent"
                        )
                    )  # list of max. discharge currents  to find minimum
                    max_charge_voltage_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/MaxChargeVoltage"
                        )
                    )  # list of max. charge voltages  to find minimum
                    charge_mode_list.append(
                        self._dbusMon.dbusmon.get_value(
                            self._batteries_dict[i], "/Info/ChargeMode"
                        )
                    )  # list of charge modes of batteries (Bulk, Absorption, Float, Keep always max voltage)

                step = "Read Allow to"
                allow_to_charge_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToCharge"
                    )
                )  # list of AllowToCharge to find minimum
                allow_to_discharge_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToDischarge"
                    )
                )  # list of AllowToDischarge to find minimum
                allow_to_balance_list.append(
                    self._dbusMon.dbusmon.get_value(
                        self._batteries_dict[i], "/Io/AllowToBalance"
                    )
                )  # list of AllowToBalance to find minimum

            step = "Find max. and min. cell voltage of all batteries"
            # placed in try-except structure for the case if some values are of None.
            # The _max() and _min() don't work with dictionaries
            max_voltage_cell_id = max(max_cell_voltage_dict, key=max_cell_voltage_dict.get)
            max_cell_voltage = max_cell_voltage_dict[max_voltage_cell_id]
            min_voltage_cell_id = min(min_cell_voltage_dict, key=min_cell_voltage_dict.get)
            min_cell_voltage = min_cell_voltage_dict[min_voltage_cell_id]

        except Exception as exc:  # copilot change: W0718
            self._readTrials += 1
            logging.error(f"{dt.now().strftime('%c')}: Error: {err}." )  # copilot change: C0209
            logging.error(f"Occured during step {step}, Battery {i}." )  # copilot change: C0209
            logging.error(f"Read trial nr. {self._readTrials}" )  # copilot change: C0209
            if self._readTrials > settings.READ_TRIALS:
                logging.error(f"{dt.now().strftime('%c')}: DBus read failed. Exiting." )  # copilot change: C0209


                
                
                sys.exit()
            else:
                return True  # next call allowed
        self._readTrials = 0  # must be reset after try-except

        #####################################################
        # Process collected values (except of dictionaries) #
        #####################################################

        # averaging
        voltage = voltage / settings.NR_OF_BATTERIES
        temperature = temperature / settings.NR_OF_BATTERIES
        voltages_sum = (
            sum(voltages_sum_dict.values()) / settings.NR_OF_BATTERIES
        )  # Marvo2011

        # find max and min cell temperature (have no ID)
        max_cell_temp = self._fn._max(max_cell_temp_list)  # copilot change: C0103
        min_cell_temp = self._fn._min(min_cell_temp_list)  # copilot change: C0103

        # find max in alarms
        low_voltage_alarm = self._fn._max(low_voltage_alarm_list)  # copilot change: C0103
        high_voltage_alarm = self._fn._max(high_voltage_alarm_list)  # copilot change: C0103
        low_cell_voltage_alarm = self._fn._max(low_cell_voltage_alarm_list)  # copilot change: C0103
        low_soc_alarm = self._fn._max(low_soc_alarm_list)  # copilot change: C0103
        high_charge_current_alarm = self._fn._max(high_charge_current_alarm_list)  # copilot change: C0103
        high_discharge_current_alarm = self._fn._max(high_discharge_current_alarm_list)  # copilot change: C0103
        cell_imbalance_alarm = self._fn._max(cell_imbalance_alarm_list)  # copilot change: C0103
        internal_failure_alarm = self._fn._max(internal_failure_alarm_list)  # copilot change: C0103
        high_charge_temperature_alarm = self._fn._max(high_charge_temperature_alarm_list)  # copilot change: C0103
        low_charge_temperature_alarm = self._fn._max(low_charge_temperature_alarm_list)  # copilot change: C0103
        high_temperature_alarm = self._fn._max(high_temperature_alarm_list)  # copilot change: C0103
        low_temperature_alarm = self._fn._max(low_temperature_alarm_list)  # copilot change: C0103
        bms_cable_alarm = self._fn._max(bms_cable_alarm_list)  # copilot change: C0103

        # find max. charge voltage (if needed)
        if not settings.OWN_CHARGE_PARAMETERS:
            max_charge_voltage = self._fn._min(max_charge_voltage_list)  # copilot change: C0103
            max_charge_current = (
                self._fn._min(max_charge_current_list) * settings.NR_OF_BATTERIES  # copilot change: C0103
            )
            max_discharge_current = (
                self._fn._min(max_discharge_current_list) * settings.NR_OF_BATTERIES  # copilot change: C0103
            )

        allow_to_charge = self._fn._min(allow_to_charge_list)  # copilot change: C0103
        allow_to_discharge = self._fn._min(allow_to_discharge_list)  # copilot change: C0103
        allow_to_balance = self._fn._min(allow_to_balance_list)  # copilot change: C0103

        ####################################
        # Measure current by Victron stuff #
        ####################################

        if settings.CURRENT_FROM_VICTRON:
            try:
                current_ve = self._dbusMon.dbusmon.get_value(
                    self._multi, "/Dc/0/Current"
                )  # copilot change: C0103
                for i in range(settings.NR_OF_MPPTS):
                    current_ve += self._dbusMon.dbusmon.get_value(
                        self._mppts_list[i], "/Dc/0/Current"
                    )  # copilot change: C0103

                if settings.DC_LOADS:
                    if settings.INVERT_SMARTSHUNT:
                        current_ve += self._dbusMon.dbusmon.get_value(
                            self._smartShunt, "/Dc/0/Current"
                        )  # SmartShunt is monitored as a battery
                    else:
                        current_ve -= self._dbusMon.dbusmon.get_value(
                            self._smartShunt, "/Dc/0/Current"
                        )

                if current_ve is not None:
                    current = current_ve  # copilot change: C0103
                    power = voltage * current_ve  # copilot change: C0103
                else:
                    logging.error(f"{dt.now().strftime('%c')}: Victron current is None. Using BMS current and power instead.")  # copilot change: C0209
            except Exception as exc:  # copilot change: W0718
                logging.error(f"{dt.now().strftime('%c')}: Victron current read error. Using BMS current and power instead. Exception: {exc}")  # copilot change: W0718
        











        ####################################################################################################
        # Calculate own charge/discharge parameters (overwrite the values received from the SerialBattery) #
        ####################################################################################################

        if settings.OWN_CHARGE_PARAMETERS:
            cvl_normal = (
                settings.NR_OF_CELLS_PER_BATTERY
                * settings.CHARGE_VOLTAGE_LIST[int((dt.now()).strftime("%m")) - 1]
            )  # copilot change: C0103
            cvl_balancing = (
                settings.NR_OF_CELLS_PER_BATTERY * settings.BALANCING_VOLTAGE
            )  # copilot change: C0103
            charge_voltage_battery = cvl_normal  # copilot change: C0103

            time_unbalanced = (
                int((dt.now()).strftime("%j")) - self._last_balancing
            )  # in days
            if time_unbalanced < 0:
                time_unbalanced += 365  # year change

            if (
                cvl_balancing > cvl_normal
            ):  # if the normal charging voltage is lower then 100% SoC
                # manage balancing voltage
                if (self._balancing == 0) and (
                    time_unbalanced >= settings.BALANCING_REPETITION
                ):
                    self._balancing = 1  # activate increased CVL for balancing
                    logging.info(f"{dt.now().strftime('%c')}: CVL increase for balancing activated.")  # copilot change: C0209







                if self._balancing == 1:
                    ChargeVoltageBattery = CVL_BALANCING
                    if (voltage >= CVL_BALANCING) and (
                        (max_cell_voltage - min_cell_voltage) < settings.CELL_DIFF_MAX
                    ):
                        self._balancing = 2
                        logging.info(f"{dt.now().strftime('%c')}: Balancing goal reached." )  # copilot change: C0209





                if self._balancing >= 2:
                    # keep balancing voltage at balancing day until decrease of solar powers and
                    ChargeVoltageBattery = CVL_BALANCING
                    if voltage <= CVL_NORMAL:  # the charge above "normal" is consumed
                        self._balancing = 0
                        self._last_balancing = int((dt.now()).strftime("%j"))  # copilot change: C0303
                        self._last_balancing_file = open(
                            "/data/dbus-aggregate-batteries/last_balancing", "w"
                        )
                        self._last_balancing_file.write("%s" % self._last_balancing)
                        self._last_balancing_file.close()
                        logging.info(f"{dt.now().strftime('%c')}: CVL increase for balancing de-activated." )  # copilot change: C0209




                if self._balancing == 0:
                    ChargeVoltageBattery = CVL_NORMAL

            elif (
                (time_unbalanced > 0)
                and (voltage >= CVL_BALANCING)
                and ((max_cell_voltage - min_cell_voltage) < settings.CELL_DIFF_MAX)
            ):  # if normal charging voltage is 100% SoC and balancing is finished
                logging.info(f"{dt.now().strftime('%c')}: Balancing goal reached with full charging set as normal. Updating last_balancing file." )  # copilot change: C0209



                
                
                
                self._last_balancing = int((dt.now()).strftime("%j"))  # copilot change: C0303
                self._last_balancing_file = open(
                    "/data/dbus-aggregate-batteries/last_balancing", "w"
                )
                self._last_balancing_file.write("%s" % self._last_balancing)
                self._last_balancing_file.close()

            if voltage >= cvl_balancing:
                self._own_charge = installed_capacity  # copilot change: C0103

            # manage dynamic CVL reduction
            if max_cell_voltage >= settings.MAX_CELL_VOLTAGE:
                if not self._dynamic_cvl:
                    self._dynamic_cvl = True
                    logging.info(f"{dt.now().strftime('%c')}: Dynamic CVL reduction started." )  # copilot change: C0209







                    if (
                        self._dc_feed_active is False
                    ):  # copilot change: C0103
                        self._dc_feed_active = self._dbusMon.dbusmon.get_value(
                            "com.victronenergy.settings",
                            "/Settings/CGwacs/OvervoltageFeedIn",
                        )  # copilot change: C0103

                self._dbusMon.dbusmon.set_value(
                    "com.victronenergy.settings",
                    "/Settings/CGwacs/OvervoltageFeedIn",
                    0,
                )  # disable DC-coupled PV feed-in
                logging.info(f"{dt.now().strftime('%c')}: DC-coupled PV feed-in de-activated." )  # copilot change: C0209






                max_charge_voltage = min(
                    (min(charge_voltage_reduced_list)), charge_voltage_battery
                )  # copilot change: C0103
            else:
                max_charge_voltage = charge_voltage_battery  # copilot change: C0103
                if self._dynamic_cvl:
                    self._dynamic_cvl = False
                    logging.info(f"{dt.now().strftime('%c')}: Dynamic CVL reduction finished." )  # copilot change: C0209







                if (
                    (max_cell_voltage - min_cell_voltage) < settings.CELL_DIFF_MAX
                ) and self._dc_feed_active:  # copilot change: C0103
                    self._dbusMon.dbusmon.set_value(
                        "com.victronenergy.settings",
                        "/Settings/CGwacs/OvervoltageFeedIn",
                        1,
                    )  # enable DC-coupled PV feed-in
                    logging.info(f"{dt.now().strftime('%c')}: DC-coupled PV feed-in re-activated." )  # copilot change: C0209
                    
                    
                    
                    
                    
                    # reset to prevent permanent logging and activation of  /Settings/CGwacs/OvervoltageFeedIn
                    self._dc_feed_active = False  # copilot change: C0103

            if (min_cell_voltage <= settings.MIN_CELL_VOLTAGE) and settings.ZERO_SOC:
                self._own_charge = 0  # copilot change: C0103

            # manage charge current
            if nr_of_modules_blocking_charge > 0:
                max_charge_current = 0  # copilot change: C0103
            else:
                max_charge_current = settings.MAX_CHARGE_CURRENT * self._fn._interpolate(
                    settings.CELL_CHARGE_LIMITING_VOLTAGE,
                    settings.CELL_CHARGE_LIMITED_CURRENT,
                    max_cell_voltage,
                )  # copilot change: C0103

            # manage discharge current
            if min_cell_voltage <= settings.MIN_CELL_VOLTAGE:
                self._fully_discharged = True  # copilot change: C0103
            elif (
                min_cell_voltage
                > settings.MIN_CELL_VOLTAGE + settings.MIN_CELL_HYSTERESIS
            ):
                self._fully_discharged = False  # copilot change: C0103

            if (nr_of_modules_blocking_discharge > 0) or (self._fully_discharged):
                max_discharge_current = 0  # copilot change: C0103
            else:
                max_discharge_current = (
                    settings.MAX_DISCHARGE_CURRENT
                    * self._fn._interpolate(
                        settings.CELL_DISCHARGE_LIMITING_VOLTAGE,
                        settings.CELL_DISCHARGE_LIMITED_CURRENT,
                        min_cell_voltage,
                    )
                )  # copilot change: C0103
                
        ###########################################################
        # own Coulomb counter (runs even the BMS values are used) #
        ###########################################################

        deltaTime = tt.time() - self._timeOld
        self._timeOld = tt.time()
        if current > 0:
            self._ownCharge += (
                current * (deltaTime / 3600) * settings.BATTERY_EFFICIENCY
            )  # charging (with efficiency)
        else:
            self._ownCharge += current * (deltaTime / 3600)  # discharging
        self._ownCharge = max(self._ownCharge, 0)
        self._ownCharge = min(self._ownCharge, installed_capacity)

        # store the charge into text file if changed significantly (avoid frequent file access)
        if abs(self._ownCharge - self._ownCharge_old) >= (
            settings.CHARGE_SAVE_PRECISION * installed_capacity
        ):
            self._charge_file = open("/data/dbus-aggregate-batteries/charge", "w")
            self._charge_file.write("%.3f" % self._ownCharge)
            self._charge_file.close()
            self._ownCharge_old = self._ownCharge

        # overwrite BMS charge values
        if settings.OWN_SOC:
            capacity = self._ownCharge
            soc = 100 * self._ownCharge / installed_capacity
            consumed_amphours = installed_capacity - self._ownCharge
            if (
                self._dbusMon.dbusmon.get_value(
                    "com.victronenergy.system", "/SystemState/LowSoc"
                )
                == 0
            ) and (current < 0):
                time_to_go = -3600 * self._ownCharge / current
            else:
                time_to_go = None
        else:
            soc = soc / installed_capacity  # weighted sum
            if time_to_go is not None:
                time_to_go = time_to_go / installed_capacity  # weighted sum

        #######################
        # Send values to DBus #
        #######################

        with self._dbusservice as bus:

            # send DC
            bus["/Dc/0/Voltage"] = voltage  # round(Voltage, 2)
            bus["/Dc/0/Current"] = current  # round(Current, 1)
            bus["/Dc/0/Power"] = power  # round(Power, 0)

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
                for cellId, currentCell in enumerate(cell_voltages_dict):
                    bus[
                        "/Voltages/%s" % (re.sub("[^A-Za-z0-9_]+", "", currentCell))
                    ] = cell_voltages_dict[currentCell]

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
            # bus['/Alarms/HighCellVoltage'] = HighCellVoltage_alarm   # not implemended in Venus
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

            bus["/Info/MaxChargeCurrent"] = max_charge_current
            bus["/Info/MaxDischargeCurrent"] = max_discharge_current
            bus["/Info/MaxChargeVoltage"] = max_charge_voltage

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

        # ##########################################################
        # ################ Periodic logging ########################
        # ##########################################################

        if settings.LOG_PERIOD > 0:
            if self._logTimer < settings.LOG_PERIOD:
                self._logTimer += 1
            else:
                self._logTimer = 0
                logging.info(
                    "%s: Repetitive logging:", dt.now().strftime('%c')  # copilot change: W1201
                )
                logging.info(
                    "  CVL: %.1fV, CCL: %.0fA, DCL: %.0fA", max_charge_voltage, max_charge_current, max_discharge_current  # copilot change: W1201
                )

                logging.info(
                    "  Bat. voltage: %sV, Bat. current: %sA, SoC: %s%%, Balancing state: %s" % (  # copilot change: W1201
                        round(voltage, 1),
                        round(current, 0),
                        round(soc, 1),
                        self._balancing,
                    )
                )

                logging.info(
                    "  Min. cell voltage: %s: %sV, Max. cell voltage: %s: %sV, difference: %sV" % (  # copilot change: W1201
                        min_voltage_cell_id,
                        round(min_cell_voltage, 3),
                        max_voltage_cell_id,
                        round(max_cell_voltage, 3),
                        round(max_cell_voltage - min_cell_voltage, 3),
                    )
                )        
        
        
        
        
        
        
        
        
        
        
        
        return True


# ################
# ################
# ## Main loop ###
# ################
# ################


def main():
    """Main entry point for the script."""  # copilot change: C0116

    logging.basicConfig(level=logging.INFO)
    logging.info(f"{dt.now().strftime('%c')}: Starting AggregateBatteries." )  # copilot change: C0209
    from dbus.mainloop.glib import DBusGMainLoop

    DBusGMainLoop(set_as_default=True)

    DbusAggBatService()

    logging.info(f"{dt.now().strftime('%c')}: Connected to dbus, and switching over to D-Bus main loop.")  # copilot change: C0303

        mainloop = GLib.MainLoop()
        mainloop.run()


if __name__ == "__main__":
    main()
