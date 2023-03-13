"""CSC343 Assignment 2

=== CSC343 Winter 2023 ===
Department of Computer Science,
University of Toronto

This code is provided solely for the personal and private use of
students taking the CSC343 course at the University of Toronto.
Copying for purposes other than this use is expressly prohibited.
All forms of distribution of this code, whether as given or with
any changes, are expressly prohibited.

Authors: Danny Heap, Marina Tawfik, and Jacqueline Smith

All of the files in this directory and all subdirectories are:
Copyright (c) 2023 Danny Heap and Jacqueline Smith

=== Module Description ===

This file contains the WasteWrangler class and some simple testing functions.
"""

import datetime as dt
import psycopg2 as pg
import psycopg2.extensions as pg_ext
import psycopg2.extras as pg_extras
from typing import Optional, TextIO

class WasteWrangler:
    """A class that can work with data conforming to the schema in
    waste_wrangler_schema.ddl.

    === Instance Attributes ===
    connection: connection to a PostgreSQL database of a waste management
    service.

    Representation invariants:
    - The database to which connection is established conforms to the schema
      in waste_wrangler_schema.ddl.
    """
    connection: Optional[pg_ext.connection]

    def __init__(self) -> None:
        """Initialize this WasteWrangler instance, with no database connection
        yet."""
        self.connection = None

    def connect(self, dbname: str, username: str, password: str) -> bool:
        """Establish a connection to the database <dbname> using the
        username <username> and password <password>, and assign it to the
        instance attribute <connection>. In addition, set the search path
        to waste_wrangler.

        Return True if the connection was made successfully, False otherwise.
        I.e., do NOT throw an error if making the connection fails."""
        try:
            self.connection = pg.connect(
                dbname=dbname, user=username, password=password,
                options="-c search_path=waste_wrangler",
                cursor_factory=pg_extras.DictCursor
            )
            return True
        except:
            return False

    def disconnect(self) -> bool:
        """Close this WasteWrangler's connection to the database.

        Return True if closing the connection was successful, False otherwise.
        I.e., do NOT throw an error if closing the connection failed."""
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            return True
        except:
            return False

    def schedule_trip(self, rid: int, time: dt.datetime) -> bool:
        """Schedule a truck and two employees to the route identified
        with <rid> at the given time stamp <time> to pick up an
        unknown volume of waste, and deliver it to the appropriate facility."""
        
        try:
            ''' Check if rid is an invalid route ID '''
            route_details = None
            with self.connection.cursor() as curs:
                curs.execute("SELECT * FROM Route WHERE rID=%s" % (rid))
                route_details = curs.fetchone()
            if route_details is None:
                self.connection.rollback()
                return False
            ''' If rid is valid, get its corresponding waste_type and length '''
            selected_rID, waste_type, length = route_details
            
            ''' Check if route has already been scheduled for today. If so, no need to schedule another trip '''
            is_route_scheduled_today = False
            with self.connection.cursor() as curs:
                curs.execute("SELECT rID FROM Trip WHERE rID=%s AND DATE(tTIME)='%s'" % (selected_rID, time.date()))
                is_route_scheduled_today = curs.fetchone() is not None
            if is_route_scheduled_today:
                self.connection.rollback()
                return False

            ''' Check if trip can start and end within working hours. If not, don't schedule the trip '''
            start_time = time
            trip_time=int((length/5)*3600)
            end_time = start_time+dt.timedelta(seconds=trip_time)
            if start_time.time() < dt.time(hour=8) or end_time.time() > dt.time(hour=16): return False

            ''' Calculate range of times for which drivers/trucks should be free '''
            lower_limit = start_time-dt.timedelta(minutes=30)
            upper_limit = end_time+dt.timedelta(minutes=30)

            with self.connection.cursor() as curs:
                ''' 
                1) First find all trucks that are able to carry the waste type of our route
                2) Select all those trucks from (1) that are on a trip in the [start-30mins end+30mins] range
                3) Then, select all trucks from (1) that are under maintenance on the same day as today
                4) Finally, trucks that are available is (1) - ((2)+(3))
                '''
                curs.execute("CREATE TEMPORARY VIEW CandidateTrucks AS SELECT truckType, tID, capacity FROM Truck NATURAL JOIN TruckType WHERE wasteType='%s'" % (waste_type))
                curs.execute("CREATE TEMPORARY VIEW TrucksDoingTrips AS SELECT CT.truckType, CT.tID, CT.capacity FROM Trip T NATURAL JOIN CandidateTrucks CT WHERE T.tTIME>'%s' AND T.tTIME<'%s'" % (lower_limit, upper_limit))
                curs.execute("CREATE TEMPORARY VIEW SameDayMaintenance AS SELECT CT.truckType, CT.tID, CT.capacity FROM Maintenance M NATURAL JOIN CandidateTrucks CT WHERE M.mDATE='%s'" % (start_time.date()))
                curs.execute("CREATE TEMPORARY VIEW AvailableTrucks AS (SELECT * FROM CandidateTrucks) EXCEPT ((SELECT * FROM TrucksDoingTrips) UNION (SELECT * FROM SameDayMaintenance))")

                ''' 
                1) First find all drivers to the trucks that are available
                2) Select all those drivers from (1) that are on a trip in the [start-30mins end+30mins] range
                3) Finally, drivers that are available is (1) - (2)
                '''
                curs.execute("CREATE TEMPORARY VIEW AllSameTruckTypeDrivers AS SELECT E.eID, E.hireDate, AT.truckType, AT.tID, AT.capacity FROM Driver NATURAL JOIN AvailableTrucks AT NATURAL JOIN Employee E WHERE '%s'>=E.hireDate" % (start_time.date()))
                curs.execute("CREATE TEMPORARY VIEW SameTruckTypeDriversDoingTrips AS SELECT CD.eID FROM Trip T, AllSameTruckTypeDrivers CD WHERE T.tTIME>'%s' AND T.tTIME<'%s' AND (T.eID1=CD.eID OR T.eID2=CD.eID)" % (lower_limit, upper_limit))
                curs.execute("CREATE TEMPORARY VIEW AvailableDrivers AS (SELECT eID FROM AllSameTruckTypeDrivers) EXCEPT (SELECT * FROM SameTruckTypeDriversDoingTrips)")
                
                '''
                Join all available trucks with drivers who can drive them
                Then perform the ranking criterion preferring
                Higher capacity, lower truckID, earlier hire dates and lower eID
                '''
                curs.execute("CREATE TEMPORARY VIEW TruckDriverPair AS SELECT tID, eID FROM AvailableDrivers NATURAL JOIN AllSameTruckTypeDrivers ORDER BY capacity desc, tID asc, hireDate asc, eID asc")
                
                ''' If no such truck-driver pairs are available, then trip cannot be scheduled '''
                curs.execute("SELECT * FROM TruckDriverPair")
                available_pairs = curs.fetchall()
                if len(available_pairs)==0:
                    self.connection.rollback()
                    return False
                
                ''' Take note of the truck and its driver that have been selected '''
                selected_tID, selected_eID1 = available_pairs[0]

                '''
                1) Find any driver i.e. can/cannot drive the truck who are not available in the timeframe
                2) Remove those from all drivers to get available drivers
                '''
                curs.execute("CREATE TEMPORARY VIEW DriversDoingTrips AS SELECT D.eID FROM Trip T, Driver D WHERE T.tTIME>='%s' AND T.tTIME<='%s' AND (T.eID1=D.eID OR T.eID2=D.eID)" % (lower_limit, upper_limit))
                curs.execute("CREATE TEMPORARY VIEW FreeDrivers AS ((SELECT eID FROM Driver) EXCEPT (SELECT * FROM DriversDoingTrips))")

                '''
                1) Exclude the selected driver and find next best driver based on more experience and smaller eID
                2) If no second driver is available, cannot schedule trip
                '''
                curs.execute("SELECT eID FROM FreeDrivers NATURAL JOIN Employee WHERE eID!=%s AND '%s'>=hireDate ORDER BY hireDate asc, eID asc" % (selected_eID1, start_time.date()))
                second_drivers = curs.fetchall()
                if len(second_drivers)==0:
                    self.connection.rollback()
                    return False
                
                ''' Take note of the second driver '''
                selected_eID2 = second_drivers[0][0]

                ''' Get facility of matching waste_type and prefer those with smaller fIDs. If none, then do not schedule '''
                curs.execute("SELECT fID FROM Facility WHERE wasteType='%s' ORDER BY fID asc" % (waste_type))
                facilities = curs.fetchall()
                if len(facilities)==0:
                    self.connection.rollback()
                    return False
                
                ''' Take note of the facility '''
                select_fID = facilities[0][0]

                '''
                All requirements satisfied. Create trip tuple to be inserted
                We compare the two eIDs to make sure that the first eID1 > eID2
                Finally, we insert the tuple and commit the transaction
                '''
                trip_tuple = None
                if(selected_eID1 > selected_eID2):
                    trip_tuple = tuple([selected_rID, selected_tID, start_time, "NULL", selected_eID1, selected_eID2, select_fID])
                else:
                    trip_tuple = tuple([selected_rID, selected_tID, start_time, "NULL", selected_eID2, selected_eID1, select_fID])
                curs.execute("INSERT INTO Trip VALUES (%s, %s, '%s', %s, %s, %s, %s)" % trip_tuple)

                curs.execute("DROP VIEW IF EXISTS FreeDrivers")
                curs.execute("DROP VIEW IF EXISTS DriversDoingTrips")
                curs.execute("DROP VIEW IF EXISTS TruckDriverPair")
                curs.execute("DROP VIEW IF EXISTS AvailableDrivers")
                curs.execute("DROP VIEW IF EXISTS SameTruckTypeDriversDoingTrips")
                curs.execute("DROP VIEW IF EXISTS AllSameTruckTypeDrivers")
                curs.execute("DROP VIEW IF EXISTS AvailableTrucks")
                curs.execute("DROP VIEW IF EXISTS SameDayMaintenance")
                curs.execute("DROP VIEW IF EXISTS TrucksDoingTrips")
                curs.execute("DROP VIEW IF EXISTS CandidateTrucks")
                self.connection.commit()
    
            return True
                
        except:
            self.connection.rollback()
            return False

    def schedule_trips(self, tid: int, date: dt.date) -> int:
        """Schedule the truck identified with <tid> for trips on <date> using
        the following approach:
        """
        
        ''' Representing how many trips were scheduled successfully '''
        successfully_scheduled=0
        
        try:
            selected_tID=tid
            routes_to_schedule = list()

            '''
            1) Get routes that do not have a trip scheduled on them today
            2) From (1), select those that can be driven on by given tID
            '''
            with self.connection.cursor() as curs:
                curs.execute("CREATE TEMPORARY VIEW EmptyRoutes AS (SELECT rID FROM Route) EXCEPT (SELECT rID FROM Trip WHERE DATE(tTIME)='%s')" % (date))
                curs.execute("CREATE TEMPORARY VIEW RoutesToSchedule AS SELECT rID FROM EmptyRoutes NATURAL JOIN Route NATURAL JOIN TruckType NATURAL JOIN Truck WHERE tID=%s ORDER BY rID asc" % (tid))
                curs.execute("SELECT * FROM RoutesToSchedule")
                routes_to_schedule = curs.fetchall()

            ''' If no such routes, then no trips to schedule, hence return 0 '''
            if len(routes_to_schedule)==0:
                self.connection.rollback()
                return 0

            ''' Represents the start_time to consider, the end_time of any given trip and the end of work_day '''
            start_time=dt.datetime(date.year, date.month, date.day, 8, 0, 0, 0)
            end_time=None
            max_time=dt.datetime(date.year, date.month, date.day, 16, 0, 0, 0)

            ''' Iterate through all routes to schedule and see which ones can actually be scheduled '''
            for rID in routes_to_schedule:
                ''' Take note of the route and the time at which that trip is supposed to start '''
                selected_rID=rID[0]
                if end_time is None: selected_time=start_time
                else: selected_time=end_time+dt.timedelta(minutes=30)

                '''
                Get the length of the route and calculate time required to complete trip
                If the trip ends after end of work day, do not schedule and continue to next route
                '''
                route_length=None
                with self.connection.cursor() as curs:
                    curs.execute("SELECT length FROM Route WHERE rID=%s" % (selected_rID))
                    route_length = curs.fetchall()[0][0]
                trip_time=int((route_length/5)*3600)
                trip_time=dt.timedelta(seconds=trip_time)
                if(start_time+trip_time>max_time):
                    self.connection.rollback()
                    continue
                
                first_drivers = list()
                selected_eID1=None
                second_drivers = list()
                selected_eID2=None
                selected_fID=None
                with self.connection.cursor() as curs:
                    '''
                    1) Find those drivers that were hired before trip date that can also drive the given truck
                    2) Find all those drivers who are busy with trips on that day
                    3) Remove all those drivers who are busy from (1) to get free driver
                    4) Prefer those who were hired earlier and have lower eID
                    '''
                    curs.execute("CREATE TEMPORARY VIEW ValidTruckTypeDrivers AS SELECT eID FROM Truck NATURAL JOIN Driver NATURAL JOIN Employee WHERE tID=%s AND %s>=hireDate", (selected_tID, date))
                    curs.execute("CREATE TEMPORARY VIEW BusyDriverPairs AS SELECT eID1, eID2 FROM Trip WHERE DATE(tTIME)='%s'" % (date))
                    curs.execute("CREATE TEMPORARY VIEW BusyDrivers AS (SELECT eID1 FROM BusyDriverPairs) UNION (SELECT eID2 FROM BusyDriverPairs)")
                    curs.execute("CREATE TEMPORARY VIEW FirstDrivers AS (SELECT * FROM ValidTruckTypeDrivers) EXCEPT (SELECT * FROM BusyDrivers)")
                    curs.execute("SELECT * FROM FirstDrivers NATURAL JOIN EMPLOYEE ORDER BY hireDate asc, eID asc")

                    ''' If no drivers are available, return out of the function '''
                    first_drivers = curs.fetchall()
                    if len(first_drivers)==0:
                        self.connection.rollback()
                        return successfully_scheduled
                    selected_eID1 = first_drivers[0][0]

                    ''' From all drivers excludig the one selected before, remove busy drivers to get the second driver '''
                    curs.execute("CREATE TEMPORARY VIEW SecondDrivers AS (SELECT eID FROM Driver WHERE eID!=%s) EXCEPT (SELECT * FROM BusyDrivers)" % (selected_eID1))
                    curs.execute("SELECT * FROM SecondDrivers NATURAL JOIN EMPLOYEE ORDER BY hireDate asc, eID asc")
                    
                    ''' If no drivers are available, return out of the function '''
                    second_drivers = curs.fetchall()
                    if len(second_drivers)==0:
                        self.connection.rollback()
                        return successfully_scheduled
                    selected_eID2 = second_drivers[0][0]

                    ''' Get fID corresponding to the route that we are scheduling and prefer smaller fIDs '''
                    curs.execute("SELECT fID FROM RoutesToSchedule NATURAL JOIN Route NATURAL JOIN Facility ORDER BY fID asc")
                    
                    ''' If no facilities are available, return out of the function '''
                    facilities = curs.fetchall()
                    if len(facilities)==0:
                        self.connection.rollback()
                        return successfully_scheduled
                    selected_fID = facilities[0][0]

                    '''
                    All requirements satisfied. Create trip tuple to be inserted
                    We compare the two eIDs to make sure that the first eID1 > eID2
                    Finally, we insert the tuple, commit the transaction and increment the counter
                    '''
                    trip_tuple = None
                    if(selected_eID1 >= selected_eID2):
                        trip_tuple = tuple([selected_rID, selected_tID, selected_time, "NULL", selected_eID1, selected_eID2, selected_fID])
                    else:
                        trip_tuple = tuple([selected_rID, selected_tID, selected_time, "NULL", selected_eID2, selected_eID1, selected_fID])
                    curs.execute("INSERT INTO Trip VALUES (%s, %s, '%s', %s, %s, %s, %s)" % trip_tuple)
                    curs.execute("DROP VIEW IF EXISTS SecondDrivers")
                    curs.execute("DROP VIEW IF EXISTS FirstDrivers")
                    curs.execute("DROP VIEW IF EXISTS BusyDrivers")
                    curs.execute("DROP VIEW IF EXISTS BusyDriverPairs")
                    curs.execute("DROP VIEW IF EXISTS ValidTruckTypeDrivers")
                    self.connection.commit()
                    successfully_scheduled+=1
            
            ''' Dropping leftover tables '''
            with self.connection.cursor() as curs:
                curs.execute("DROP VIEW IF EXISTS RoutesToSchedule")
                curs.execute("DROP VIEW IF EXISTS EmptyRoutes")
            return successfully_scheduled

        except:
            return successfully_scheduled

    def update_technicians(self, qualifications_file: TextIO) -> int:
        try:
            data= self._read_qualifications_file(qualifications_file)
            data= [tuple([e[0]+' '+e[1], e[2]]) for e in data]
            data= list(set(data))

            '''Check whether the employee exists in the Employee table'''
            with self.connection.cursor() as curs:
                curs.execute("CREATE TEMPORARY TABLE TextFile (name varchar, truckType varchar(50), UNIQUE (name, truckType))")
                curs.executemany("INSERT INTO TextFile (name, truckType) VALUES (%s, %s)", data)

                '''Consider only the names that exist in the Employee table'''
                curs.execute("CREATE TEMPORARY VIEW View1 AS SELECT eID, truckType FROM Employee NATURAL JOIN TextFile")
                '''We will only consider valid truckTypes'''
                curs.execute("CREATE TEMPORARY VIEW View2 AS SELECT eID, truckType FROM View1 NATURAL JOIN (SELECT distinct truckType FROM TruckType) AS Temp")
                '''If the technicians are already recorded to work on a truckType, we remove them'''
                curs.execute("CREATE TEMPORARY VIEW View3 AS SELECT * FROM View2 EXCEPT (SELECT eID, truckType FROM View2 NATURAL JOIN Technician)")
                '''Remove the entries where the eID is a driver'''
                curs.execute("CREATE TEMPORARY VIEW View4 AS SELECT * FROM View3 EXCEPT (SELECT eID, truckType FROM View3 NATURAL JOIN Driver)")
                curs.execute("SELECT * FROM View4")
                technicians_to_add = curs.fetchall()

                '''Return the updated technician list'''
                if len(technicians_to_add)==0:
                    self.connection.rollback()
                    return 0
                curs.executemany("INSERT INTO Technician(eID, truckType) VALUES (%s, %s)", technicians_to_add)
                curs.execute("DROP VIEW IF EXISTS View4")
                curs.execute("DROP VIEW IF EXISTS View3")
                curs.execute("DROP VIEW IF EXISTS View2")
                curs.execute("DROP VIEW IF EXISTS View1")
                curs.execute("DROP Table IF EXISTS TextFile")
                self.connection.commit()
            return len(technicians_to_add)

        except pg.Error:
            return 0
    
    def workmate_sphere(self, eid: int) -> list[int]:
        """Return the workmate sphere of the driver identified by <eid>, as a
        list of eIDs.

        The workmate sphere of <eid> is:
            * Any employee who has been on a trip with <eid>.
            * Recursively, any employee who has been on a trip with an employee
              in <eid>'s workmate sphere is also in <eid>'s workmate sphere.
        """

        try:
            ''' Check if the driver eID provided is valid '''
            does_driver_exist = False
            with self.connection.cursor() as curs:
                curs.execute("SELECT * FROM Driver WHERE eID=%s" % (eid))
                does_driver_exist = len(curs.fetchall()) != 0

            if not does_driver_exist:
                self.connection.rollback()
                return []

            ''' Find all pairs of eIDs for every trip '''
            all_eID_pairs = set()
            with self.connection.cursor() as curs:
                curs.execute("SELECT eID1, eID2 FROM Trip")
                for eID_pair in curs:
                    all_eID_pairs.add(tuple(eID_pair))
            all_eID_pairs = list(all_eID_pairs)

            ''' Helper function to find direct partners of a supplied eID '''
            def get_partners_of_given_eID(eID):
                pairs_of_given_eID = list(filter(lambda e: e[0]==eID or e[1]==eID , all_eID_pairs))
                partners = set()
                for eID_pair in pairs_of_given_eID:
                    eID1, eID2 = eID_pair
                    other_eID = eID2 if eID1==eID else eID1
                    partners.add(other_eID)
                
                return partners

            ''' Set representing iput eID, workmates of input_eID and partner eIDs of input_eID and so on '''
            input_eID = set([eid])
            workmates = set([])
            eIDs_to_check = get_partners_of_given_eID(eid)

            ''' If eIDs to check is non-empty, keep running the while loop '''
            while len(eIDs_to_check):
                ''' Remove one eID from the set and add it to the workmate_sphere '''
                eID = eIDs_to_check.pop()
                workmates.add(eID)

                ''' Get the direct partners of the above eID as those will also be workmates '''
                partners_of_eID = get_partners_of_given_eID(eID)

                ''' Remove instances of input_eID and those eIDs that exist inside workmates_sphere already '''
                partners_of_eID.difference_update(workmates, input_eID)

                ''' Append partners from above to the set that needs to be checked '''
                eIDs_to_check.update(partners_of_eID)

            ''' Once all necessary eIDs are checked, exist out of the function '''
            return list(workmates)
        
        except:
            return []
      
    def schedule_maintenance(self, date: dt.date) -> int:
        num_trucks_scheduled = 0

        try:
            cur = self.connection.cursor()
            date_90_days_ago = date - dt.timedelta(days=90)
            date_10_days_after = date + dt.timedelta(days=10)

            cur.execute("CREATE TEMPORARY VIEW MaintainedTrucks AS SELECT DISTINCT tID FROM Maintenance WHERE mDATE BETWEEN '%s' AND '%s'" % (date_90_days_ago, date_10_days_after))
            cur.execute("CREATE TEMPORARY VIEW NotMaintainedTrucks AS (SELECT tID FROM Truck) EXCEPT (SELECT * FROM MaintainedTrucks)")
            cur.execute("SELECT tID, trucktype FROM NotMaintainedTrucks NATURAL JOIN Truck ORDER BY tID asc")
            trucks_to_maintain = cur.fetchall()
            if len(trucks_to_maintain)==0:
                self.connection.rollback()
                cur.close()
                return 0

            for truck in trucks_to_maintain:
                tID, truck_type = truck

                ''' Fetch all the technicians that can maintain given truck. If none, check next truck '''
                cur.execute("SELECT eID FROM Technician WHERE truckType='%s'" % (truck_type))
                available_technicians=cur.fetchall()
                if len(available_technicians) == 0:
                    self.connection.rollback()
                    continue

                '''Iterate to find the ideal day on which maintenance can be scheduled'''
                found_date = False
                search_date = date + dt.timedelta(days=1)

                while not found_date:
                    ''' If truck scheduled for trip on given search_date, look for next day '''
                    cur.execute("SELECT * FROM Trip WHERE tID=%s AND DATE(tTIME)='%s'" % (tID, search_date))
                    if len(cur.fetchall())!=0:
                        self.connection.rollback()
                        search_date+=dt.timedelta(days=1)
                        continue

                    ''' If truck scheduled for maintenance on given search_date, look for next day '''
                    cur.execute("SELECT * FROM Maintenance WHERE tID=%s AND mDATE='%s'" % (tID, search_date))
                    if len(cur.fetchall())!=0:
                        self.connection.rollback()
                        search_date+=dt.timedelta(days=1)
                        continue

                    ''' Get all free technicians on search day and select one with smallest eID '''
                    cur.execute("CREATE TEMPORARY VIEW AvailableTechnicians AS SELECT eID FROM Technician NATURAL JOIN Employee WHERE truckType='%s' AND hireDate<='%s'" % (truck_type, search_date))
                    cur.execute("CREATE TEMPORARY VIEW BusyTechnicans AS SELECT eID FROM AvailableTechnicians NATURAL JOIN Maintenance WHERE mDATE='%s'" % (search_date))
                    cur.execute("CREATE TEMPORARY VIEW FreeTechnicians AS (SELECT * FROM AvailableTechnicians) EXCEPT (SELECT * FROM BusyTechnicans)")
                    cur.execute("SELECT * FROM FreeTechnicians ORDER BY eID asc")
                    free_technicians = cur.fetchall()
                    if len(free_technicians)==0:
                        self.connection.rollback()
                        search_date+=dt.timedelta(days=1)
                        continue
                    eID = free_technicians[0][0]
                    
                    ''' All requirements met, hence, set found flag to true and insert value '''
                    cur.execute("INSERT INTO Maintenance (tID, eID, mDATE) VALUES (%s, %s, %s)", (tID, eID, search_date))
                    found_date = True
                    num_trucks_scheduled+=1
                    cur.execute("DROP VIEW FreeTechnicians")
                    cur.execute("DROP VIEW BusyTechnicans")
                    cur.execute("DROP VIEW AvailableTechnicians")
                    self.connection.commit()

            cur.execute("DROP VIEW IF EXISTS NotMaintainedTrucks")
            cur.execute("DROP VIEW IF EXISTS MaintainedTrucks")
            cur.close()
            return num_trucks_scheduled

        except:
            return num_trucks_scheduled

    def reroute_waste(self, fid: int, date: dt.date) -> int:
        """Reroute the trips to <fid> on day <date> to another facility that
        takes the same type of waste. If there are many such facilities, pick
        the one with the smallest fID (that is not <fid>).
        """

        try:
            ''' Check if there are any trips to given facility on given date '''
            trips_to_reroute=list()
            with self.connection.cursor() as curs:
                curs.execute("SELECT * FROM TRIP WHERE fID=%s AND DATE(tTIME)='%s'" % (fid, date))
                trips_to_reroute=curs.fetchall()
            if len(trips_to_reroute)==0: return 0

            ''' Find waste_type of the facility in concern, should not fail '''
            waste_type=None
            with self.connection.cursor() as curs:
                curs.execute("SELECT wasteType FROM Facility WHERE fID=%s" % (fid))
                waste_type = curs.fetchone()[0]
            if waste_type is None: return 0

            ''' Find a list of all facilities that can act as replacement ordered by lowest eID '''
            candidate_facilities=list()
            with self.connection.cursor() as curs:
                curs.execute("SELECT fID FROM Facility WHERE wasteType='%s' AND fID!=%s ORDER BY fID asc" % (waste_type, fid))
                candidate_facilities=curs.fetchall()
            if len(candidate_facilities)==0: return 0

            selected_fID=candidate_facilities[0][0]

            with self.connection.cursor() as curs:
                curs.execute("UPDATE Trip SET fID=%s WHERE fID=%s AND DATE(tTIME)='%s'" % (selected_fID, fid, date))
            self.connection.commit()
            return len(trips_to_reroute)
        
        except:
            return 0

    # =========================== Helper methods ============================= #

    @staticmethod
    def _read_qualifications_file(file: TextIO) -> list[list[str, str, str]]:
        """Helper for update_technicians. Accept an open file <file> that
        follows the format described on the A2 handout and return a list
        representing the information in the file, where each item in the list
        includes the following 3 elements in this order:
            * The first name of the technician.
            * The last name of the technician.
            * The truck type that the technician is currently qualified to work
              on.

        Pre-condition:
            <file> follows the format given on the A2 handout.
        """
        result = []
        employee_info = []
        for idx, line in enumerate(file):
            if idx % 2 == 0:
                info = line.strip().split(' ')[-2:]
                fname, lname = info
                employee_info.extend([fname, lname])
            else:
                employee_info.append(line.strip())
                result.append(employee_info)
                employee_info = []

        return result

def setup(dbname: str, username: str, password: str, file_path: str) -> None:
    """Set up the testing environment for the database <dbname> using the
    username <username> and password <password> by importing the schema file
    and the file containing the data at <file_path>.
    """
    connection, cursor, schema_file, data_file = None, None, None, None
    try:
        # Change this to connect to your own database
        connection = pg.connect(
            dbname=dbname, user=username, password=password,
            options="-c search_path=waste_wrangler"
        )
        cursor = connection.cursor()

        schema_file = open("./waste_wrangler_schema.sql", "r")
        cursor.execute(schema_file.read())

        data_file = open(file_path, "r")
        cursor.execute(data_file.read())

        connection.commit()
    except Exception as ex:
        connection.rollback()
        raise Exception(f"Couldn't set up environment for tests: \n{ex}")
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if connection and not connection.closed:
            connection.close()
        if schema_file:
            schema_file.close()
        if data_file:
            data_file.close()

def test_preliminary() -> None:
    """Test preliminary aspects of the A2 methods."""
    ww = WasteWrangler()
    qf = None
    try:
        # TODO: Change the values of the following variables to connect to your
        #  own database:
        dbname = 'csc343h-desaihi2'
        user = ''
        password = ''

        connected = ww.connect(dbname, user, password)

        # The following is an assert statement. It checks that the value for
        # connected is True. The message after the comma will be printed if
        # that is not the case (connected is False).
        # Use the same notation to thoroughly test the methods we have provided
        assert connected, f"[Connected] Expected True | Got {connected}."

        # TODO: Test one or more methods here, or better yet, make more testing
        #   functions, with each testing a different aspect of the code.

        # The following function will set up the testing environment by loading
        # the sample data we have provided into your database. You can create
        # more sample data files and use the same function to load them into
        # your database.
        # Note: make sure that the schema and data files are in the same
        # directory (folder) as your a2.py file.
        setup(dbname, user, password, './waste_wrangler_data.sql')

        # --------------------- Testing schedule_trip  ------------------------#

        # You will need to check that data in the Trip relation has been
        # changed accordingly. The following row would now be added:
        # (1, 1, '2023-05-04 08:00', null, 2, 1, 1)
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 8, 0))
        assert scheduled_trip, \
            f"[Schedule Trip] Expected True, Got {scheduled_trip}"

        # Can't schedule the same route of the same day.
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 13, 0))
        assert not scheduled_trip, \
            f"[Schedule Trip] Expected False, Got {scheduled_trip}"

        # -------------------- Testing schedule_trips  ------------------------#

        # All routes for truck tid are scheduled on that day
        scheduled_trips = ww.schedule_trips(1, dt.datetime(2023, 5, 3))
        assert scheduled_trips == 0, \
            f"[Schedule Trips] Expected 0, Got {scheduled_trips}"

        # ----------------- Testing update_technicians  -----------------------#

        # This uses the provided file. We recommend you make up your custom
        # file to thoroughly test your implementation.
        # You will need to check that data in the Technician relation has been
        # changed accordingly
        qf = open('qualifications.txt', 'r')
        updated_technicians = ww.update_technicians(qf)
        assert updated_technicians == 2, \
            f"[Update Technicians] Expected 2, Got {updated_technicians}"

        # ----------------- Testing workmate_sphere ---------------------------#

        # This employee doesn't exist in our instance
        workmate_sphere = ww.workmate_sphere(2023)
        assert len(workmate_sphere) == 0, \
            f"[Workmate Sphere] Expected [], Got {workmate_sphere}"

        workmate_sphere = ww.workmate_sphere(3)
        # Use set for comparing the results of workmate_sphere since
        # order doesn't matter.
        # Notice that 2 is added to 1's work sphere because of the trip we
        # added earlier.
        assert set(workmate_sphere) == {1, 2}, \
            f"[Workmate Sphere] Expected {{1, 2}}, Got {workmate_sphere}"

        # ----------------- Testing schedule_maintenance ----------------------#

        # You will need to check the data in the Maintenance relation
        scheduled_maintenance = ww.schedule_maintenance(dt.date(2023, 5, 5))
        assert scheduled_maintenance == 7, \
            f"[Schedule Maintenance] Expected 7, Got {scheduled_maintenance}"

        # ------------------ Testing reroute_waste  ---------------------------#

        # There is no trips to facility 1 on that day
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 10))
        assert reroute_waste == 0, \
            f"[Reroute Waste] Expected 0. Got {reroute_waste}"

        # You will need to check that data in the Trip relation has been
        # changed accordingly
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 3))
        assert reroute_waste == 1, \
            f"[Reroute Waste] Expected 1. Got {reroute_waste}"
    finally:
        if qf and not qf.closed:
            qf.close()
        ww.disconnect()

if __name__ == '__main__':
    # TODO: Put your testing code here
    test_preliminary()