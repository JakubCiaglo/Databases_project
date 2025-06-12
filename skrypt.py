import mysql.connector
import random
import math
from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from faker import Faker
from collections import defaultdict


def uruchom():
    """Główna funkcja uruchamiająca cały skrypt populacji bazy danych."""
    
    # Sekcja 2: Połączenie z bazą danych i wyczyszczenie jej
    print("--- ETAP 1: Łączenie z bazą i czyszczenie tabel ---")
    con = mysql.connector.connect(
        host = "giniewicz.it",
        user = "team13",
        password = "te@mlie",
        database = "team13"
    )

    if(con):
        print("Połączenie udane")
    else:
        print("Połączenie nieudane")

    cursor = con.cursor()
    
    cursor.execute('SET FOREIGN_KEY_CHECKS = 0;')
    
    tables = [
        'employee_assignments',
        'feedback',
        'transactions',
        'trip_participants',
        'incidents',
        'costs',
        'trips',
        'trip_types',
        'destinations',
        'launch_stations',
        'spacecraft',
        'rockets',
        'emergency_contacts',
        'clients',
        'employees'
    ]

    for table in tables:
        cursor.execute(f'TRUNCATE TABLE {table};')
    cursor.execute('SET FOREIGN_KEY_CHECKS = 1;')

    con.commit()
    cursor.close()
    print('Wyczyszczono wszystkie tabele.')
    print("-" * 50)

    # Sekcja 3: Wyznaczenie parametrów
    print("--- ETAP 2: Ustawianie parametrów globalnych ---")
    n_employees = 200
    clients_per_emp = 3 
    n_clients = n_employees * clients_per_emp
    min_salary_pln = 14666.00 
    avg_salary_pln = 30045.11

    current_date = datetime.now() + relativedelta(years=1000)
    start_of_business = current_date - relativedelta(years=5)

    print(f"Data rozpoczęcia działalności: {start_of_business}")
    print("-" * 50)
    
    # Sekcja 4: Generowanie pracowników
    print("--- ETAP 3: Generowanie danych pracowników ---")
    fake = Faker("pl_PL")
    rng = np.random.default_rng()

    POSITIONS = [
        ("Pilot statku", 0.07), ("Inżynier systemów", 0.12),
        ("Technik pokładowy", 0.15), ("Kontroler lotów", 0.1),
        ("Astrogator", 0.05), ("Specjalista ds. bezpieczeństwa", 0.05),
        ("Inspektor jakości", 0.06), ("Specjalista HR", 0.04),
        ("Analityk danych", 0.06), ("Marketing kosmiczny", 0.04),
        ("Finanse / Księgowość", 0.06), ("Lekarz pokładowy", 0.05),
        ("Kucharz orbitalny", 0.03), ("Administrator IT", 0.05),
        ("Mechanik rakietowy", 0.07)
    ]
    titles, weights = zip(*POSITIONS)

    def random_salary():
        raw = rng.lognormal(mean=math.log(avg_salary_pln), sigma=0.25)
        return round(max(raw, min_salary_pln), 2)

    def hire_and_term():
        end_hire_period = current_date - relativedelta(months=6)
        total_days = (end_hire_period - start_of_business).days
        
        random_days = random.randint(0, total_days)
        hire = start_of_business + timedelta(days=random_days)
        
        if random.random() < 0.15:
            min_term_date = hire + relativedelta(months=3)
            term_days = (current_date - min_term_date).days
            
            if term_days > 0:
                random_term_days = random.randint(0, term_days)
                term = min_term_date + timedelta(days=random_term_days)
            else:
                term = None
        else:
            term = None
        
        return hire, term

    emp_rows, used_emails_emp = [], set()
    print(f'Generowanie danych dla {n_employees} pracowników...')
    for i in range(n_employees):
        first, last = fake.first_name(), fake.last_name()
        position = random.choices(titles, weights)[0]
        salary = random_salary()
        hire, term = hire_and_term()
        base_email = f'{first}.{last}'.lower().replace(' ', '').replace("'", '')
        email = base_email + '@spaceu.com'
        suffix = 1
        while email in used_emails_emp:
            email = f'{base_email}{suffix}@spaceu.com'
            suffix += 1
        used_emails_emp.add(email)
        phone = fake.phone_number()
        if random.random() < 0.02:
            email = None
            phone = None
        emp_rows.append({
            'first_name': first, 'last_name': last, 'position': position,
            'salary': salary, 'hire_date': hire, 'termination_date': term,
            'email': email, 'phone': phone
        })

    df_emp = pd.DataFrame(emp_rows)
    print(f"Wygenerowano {len(df_emp)} rekordów dla pracowników.")
    
    # Sekcja 5: Generowanie klientów
    cli_rows, used_emails_cli = [], set()
    domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'protonmail.com']

    print(f"\nGenerowanie danych dla {n_clients} klientów...")
    for i in range(n_clients):
        first, last = fake.first_name(), fake.last_name()
        base = f"{first}.{last}".lower().replace(" ", "").replace("'", "")
        domain = random.choice(domains)
        email_candidate = f"{base}@{domain}"
        suffix = 1
        while email_candidate in used_emails_cli or email_candidate in used_emails_emp:
            email_candidate = f"{base}{suffix}@{domain}"
            suffix += 1
        phone = fake.phone_number()
        if random.random() < 0.02:
            email_candidate = None
            phone = None
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=75)
        shifted_birth_date = birth_date.replace(year=birth_date.year + 1000)
        cli_rows.append({
            "first_name": first, "last_name": last,
            "date_of_birth": shifted_birth_date,
            "email": email_candidate, "phone": phone
        })

    df_cli = pd.DataFrame(cli_rows)
    print(f"Wygenerowano {len(df_cli)} rekordów dla klientów.")
    print("-" * 50)
    
    # Sekcja 6: Zapis pracowników i klientów do bazy
    print("--- ETAP 4: Zapisywanie pracowników i klientów w bazie ---")
    if con and con.is_connected():
        cursor = con.cursor()
        print("\nPołączono z bazą danych. Rozpoczynanie wstawiania danych...")

        sql_insert_employee = """
        INSERT INTO employees (first_name, last_name, position, salary, hire_date, termination_date, email, phone)
        VALUES (%(first_name)s, %(last_name)s, %(position)s, %(salary)s, %(hire_date)s, %(termination_date)s, %(email)s, %(phone)s)
        """
        employee_records_to_insert = df_emp.to_dict(orient='records')
        
        try:
            cursor.executemany(sql_insert_employee, employee_records_to_insert)
            con.commit()
            print(f"{cursor.rowcount} rekordów wstawiono/zaktualizowano w tabeli employees.")
        except mysql.connector.Error as err:
            print(f"Błąd podczas wstawiania do employees: {err}")
            con.rollback()

        sql_insert_client = """
        INSERT INTO clients (first_name, last_name, date_of_birth, email, phone)
        VALUES (%(first_name)s, %(last_name)s, %(date_of_birth)s, %(email)s, %(phone)s)
        """
        client_records_to_insert = df_cli.to_dict(orient='records')

        try:
            cursor.executemany(sql_insert_client, client_records_to_insert)
            con.commit()
            print(f"{cursor.rowcount} rekordów wstawiono/zaktualizowano w tabeli clients.")
        except mysql.connector.Error as err:
            print(f"Błąd podczas wstawiania do clients: {err}")
            con.rollback()

        try:
            cursor.execute("SELECT COUNT(*) FROM employees")
            total_emp = cursor.fetchone()[0]
            print(f"Łącznie w tabeli employees: {total_emp}")

            cursor.execute("SELECT COUNT(*) FROM clients")
            total_cli = cursor.fetchone()[0]
            print(f"Łącznie w tabeli clients: {total_cli}")
        except mysql.connector.Error as err:
            print(f"Błąd podczas odczytu liczby rekordów: {err}")

        print("Zakończono operacje na bazie danych dla tego bloku.")
    else:
        print("Błąd: Połączenie z bazą danych ('con') nie jest aktywne lub nie zostało poprawnie zainicjowane.")
    print("-" * 50)

    # Sekcja 7: Generowanie statków kosmicznych i rakiet
    print("--- ETAP 5: Generowanie danych słownikowych (rakiety, statki, itp.) ---")
    def random_date(start_date=start_of_business):
        end_date = start_date + timedelta(days=2*365)
        delta_days = (end_date - start_date).days
        return start_date + timedelta(days=random.randint(0, delta_days))

    MANUFACTURERS = ["NASA", "SpaceX", "Blue Origin", "ULA", "Roscosmos", "Arianespace"]

    def generate_spacecraft_entries(n=30):
        prefixes = ['Nova', 'Luna', 'Astra', 'Zenith', 'Hyperion', 'Celestia', 'Pulsar', 'Eclipse', 'Titan', 'Vortex']
        suffixes = ['X', 'One', 'Prime', '7', 'NX', 'Infinity', 'Core', 'Ultra', 'VX', 'Edge']
        statuses = ['ACTIVE', 'RETIRED', 'MAINT']
        used_names = set()
        spacecraft = []
        if n > len(prefixes) * len(suffixes):
            raise ValueError(f"Nie da się wygenerować {n} unikalnych nazw (maksymalnie {len(prefixes)*len(suffixes)}).")
        for _ in range(n):
            while True:
                name = f"{random.choice(prefixes)}-{random.choice(suffixes)}"
                if name not in used_names:
                    used_names.add(name)
                    break
            manufacturer = random.choice(MANUFACTURERS)
            capacity = random.randint(40, 60)
            status = random.choices(statuses, weights=[0.8, 0.1, 0.1])[0]
            start_date = random_date()
            end_date = None
            if status == 'RETIRED':
                min_end_date = start_date + timedelta(days=365)
                max_end_date = min_end_date + timedelta(days=3*365)
                end_date = min_end_date + timedelta(days=random.randint(0, (max_end_date - min_end_date).days))
            spacecraft.append((capacity, name, manufacturer, start_date, end_date, status))
        return spacecraft

    def generate_rocket_entries(n=30):
        prefixes = ['Falcon', 'Vega', 'Atlas', 'Delta', 'Zephyr', 'Aegis', 'Cyclone', 'Talon', 'Nova', 'Strato']
        suffixes = ['IX', 'V', 'Max', 'Pro', 'Ultra', '1000', 'Eon', 'Zero', 'R', 'Edge']
        statuses = ['ACTIVE', 'RETIRED', 'MAINT']
        used_names = set()
        rockets = []
        if n > len(prefixes) * len(suffixes):
            raise ValueError(f"Nie da się wygenerować {n} unikalnych nazw rakiet (maksymalnie {len(prefixes)*len(suffixes)}).")
        for _ in range(n):
            while True:
                name = f"{random.choice(prefixes)}-{random.choice(suffixes)}"
                if name not in used_names:
                    used_names.add(name)
                    break
            manufacturer = random.choice(MANUFACTURERS)
            status = random.choices(statuses, weights=[0.8, 0.1, 0.1])[0]
            rockets.append((name, manufacturer, status))
        return rockets

    # Sekcja 8: Wprowadzenie danych słownikowych do bazy
    launch_stations_data = [
        ("Kennedy Space Center", "USA", "Cape Canaveral", "ACTIVE"),
        ("Baikonur Cosmodrome", "Kazakhstan", "Baikonur", "ACTIVE"),
        ("Guiana Space Centre", "France", "Kourou", "ACTIVE"),
        ("Wenchang", "China", "Wenchang", "ACTIVE"),
        ("Space-U Pad Baltic", "Poland", "Ustka", "PLANNED")
    ]
    rockets_data = generate_rocket_entries()
    spacecraft_data = generate_spacecraft_entries()
    destinations_data = [
        ("Merkury", "Najmniejsza i najbliższa Słońcu planeta Układu Słonecznego", 0.38, "HIGH"),
        ("Wenus", "Druga planeta od Słońca, o gęstej atmosferze i ekstremalnym cieple", 0.90, "HIGH"),
        ("Mars", "Czwarta planeta od Słońca, znana jako Czerwona Planeta", 0.38, "MEDIUM"),
        ("Jowisz", "Największa planeta w Układzie Słonecznym, gazowy olbrzym", 0.16, "CRITICAL"),
        ("Saturn", "Gazowy olbrzym znany z wyraźnych pierścieni", 0.92, "MEDIUM")
    ]
    trip_types_data = [
        ("Misja Orbitalna", "Orbita i obserwacja powierzchni planety", 14, 6_000_000),
        ("Zejście Atmosferyczne", "Zejście w atmosferę i analiza składu chemicznego", 21, 7_500_000),
        ("Ekspedycja Księżycowa", "Eksploracja naturalnych satelitów", 20, 9_000_000),
        ("Obóz Badawczy w Kosmosie", "Eksperymenty z pokładu bazy orbitalnej", 45, 16_000_000),
        ("Przelot przez Pasy Radiacyjne", "Przelot przez pasy radiacyjne i pomiary", 40, 10_000_000),
        ("Obserwacja Pierścieni", "Obserwacja struktur pierścieni planetarnych", 20, 12_000_000),
        ("Eksplorator Magnetosfery", "Pomiary pola magnetycznego i zorze", 30, 7_000_000),
        ("Manewr Grawitacyjny", "Test manewrów grawitacyjnych przy dużych obiektach", 10, 5_500_000)
    ]

    def insert_many(q, rows):
        cursor.executemany(q, rows)
        con.commit()

    insert_many("INSERT INTO launch_stations (name, country, city, status) VALUES (%s, %s, %s, %s)", launch_stations_data)
    insert_many("INSERT INTO rockets (name, manufacturer, status) VALUES (%s, %s, %s)", rockets_data)
    insert_many("INSERT INTO spacecraft (capacity_passengers, name, manufacturer, service_start_date, service_end_date, status) VALUES (%s, %s, %s, %s, %s, %s)", spacecraft_data)
    insert_many("INSERT INTO destinations (name, description, avg_gravity, hazard_level) VALUES (%s, %s, %s, %s)", destinations_data)
    insert_many("INSERT INTO trip_types (name, description, typical_duration_days, base_price) VALUES (%s, %s, %s, %s)", trip_types_data)
    print("► Załadowano słownikowe tabele.")
    print("-" * 50)

    # Sekcja 9: Generowanie podróży (trips)
    print("--- ETAP 6: Generowanie podróży (trips) ---")
    cursor.execute('SELECT trip_type_id, typical_duration_days FROM trip_types')
    fetched_trip_types = cursor.fetchall()
    trip_type_durations = dict(fetched_trip_types)

    cursor.execute('SELECT destination_id FROM destinations')
    destinations_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT launch_station_id FROM launch_stations WHERE status = 'ACTIVE'")
    launch_stations_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT spacecraft_id FROM spacecraft WHERE status = 'ACTIVE'")
    spacecrafts_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT rocket_id FROM rockets WHERE status = 'ACTIVE'")
    rockets_ids = [row[0] for row in cursor.fetchall()]

    statuses = ['completed', 'in progress', 'planned']
    spacecraft_schedule = { sc: [] for sc in spacecrafts_ids }
    rocket_schedule = { rk: [] for rk in rockets_ids }
    records_to_insert_trips = []

    def is_overlapping(new_start, new_end, existing_intervals):
        new_end = new_end or datetime.max.replace(year=datetime.max.year - 1)
        for ex_start, ex_end in existing_intervals:
            ex_end = ex_end or datetime.max.replace(year=datetime.max.year - 1)
            if not (new_end < ex_start or ex_end < new_start):
                return True
        return False

    for _ in range(50):
        trip_type_id = random.choice(list(trip_type_durations.keys()))
        duration = trip_type_durations[trip_type_id]
        destination_id = random.choice(destinations_ids)
        launch_station_id = random.choice(launch_stations_ids)
        status = random.choices(statuses, weights=[0.7, 0.1, 0.2])[0]
        if status == 'completed':
            total_days = (current_date - start_of_business).days - duration
            total_days = max(total_days, 0)
            base_date = start_of_business + timedelta(days=random.randint(0, total_days))
            departure = datetime(base_date.year, base_date.month, base_date.day, random.randint(0,23), random.randint(0,59), random.randint(0,59))
            return_date = base_date + timedelta(days=duration)
            return_dt = datetime(return_date.year, return_date.month, return_date.day, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        elif status == 'in progress':
            days_back = random.randint(1, duration)
            rand_dt = current_date - timedelta(days=days_back)
            departure = datetime(rand_dt.year, rand_dt.month, rand_dt.day, random.randint(0,23), random.randint(0,59), random.randint(0,59))
            return_dt = None
        else:
            days_forward = random.randint(1, 365)
            future_dt = current_date + timedelta(days=days_forward)
            departure = datetime(future_dt.year, future_dt.month, future_dt.day, random.randint(0,23), random.randint(0,59), random.randint(0,59))
            return_dt = None
        new_start = departure
        new_end = return_dt if return_dt is not None else datetime.max.replace(year=current_date.year + 100)
        chosen_spacecraft = None
        chosen_rocket = None
        for attempt in range(100):
            sc = random.choice(spacecrafts_ids)
            if is_overlapping(new_start, new_end, spacecraft_schedule[sc]):
                continue
            rk = random.choice(rockets_ids)
            if is_overlapping(new_start, new_end, rocket_schedule[rk]):
                continue
            chosen_spacecraft = sc
            chosen_rocket = rk
            break
        if chosen_spacecraft is None:
            raise RuntimeError("Nie znaleziono wolnego statku + rakiety. Zmniejsz liczbę lotów lub zasobów.")
        spacecraft_schedule[chosen_spacecraft].append((new_start, new_end))
        rocket_schedule[chosen_rocket].append((new_start, new_end))
        records_to_insert_trips.append((trip_type_id, destination_id, launch_station_id, chosen_spacecraft, chosen_rocket, departure, return_dt, status))

    cursor.executemany("INSERT INTO trips (trip_type_id, destination_id, launch_station_id, spacecraft_id, rocket_id, departure_datetime, return_datetime, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", records_to_insert_trips)
    con.commit()
    print(f"Wygenerowano i wstawiono {len(records_to_insert_trips)} podróży.")
    print("-" * 50)
    
    # Sekcja 10: Generowanie pracowników do lotu
    print("--- ETAP 7: Przypisywanie pracowników do lotów ---")
    excluded_positions = ['Finanse / Księgowość', 'Analityk danych', 'Marketing kosmiczny', 'Administrator IT', 'Specjalista HR']
    cursor.execute("SELECT employee_id, position FROM employees WHERE position NOT IN (%s)" % ','.join(['%s'] * len(excluded_positions)), excluded_positions)
    crew_data = cursor.fetchall()
    
    position_map = defaultdict(list)
    for emp_id, pos in crew_data:
        position_map[pos].append(emp_id)

    cursor.execute('SELECT trip_id, departure_datetime, return_datetime FROM trips')
    trip_info = cursor.fetchall()

    employee_schedule = defaultdict(list)
    assignments = []

    for trip_id, departure, return_dt in trip_info:
        selected_employees = set()
        trip_assignment = []
        new_start = departure
        new_end = return_dt or datetime.max.replace(year=datetime.max.year - 1)
        for position, emp_list in position_map.items():
            random.shuffle(emp_list)
            for emp_id in emp_list:
                if not is_overlapping(new_start, new_end, employee_schedule[emp_id]):
                    selected_employees.add(emp_id)
                    employee_schedule[emp_id].append((new_start, new_end))
                    trip_assignment.append((trip_id, emp_id))
                    break
        
        remaining_pool = [(emp_id, pos) for pos, emp_ids in position_map.items() for emp_id in emp_ids if emp_id not in selected_employees and not is_overlapping(new_start, new_end, employee_schedule[emp_id])]
        additional_needed = 35 - len(trip_assignment)
        if additional_needed > 0:
            random.shuffle(remaining_pool)
            additional_crew = remaining_pool[:additional_needed]
            for emp_id, pos in additional_crew:
                selected_employees.add(emp_id)
                employee_schedule[emp_id].append((new_start, new_end))
                trip_assignment.append((trip_id, emp_id))
        assignments.extend(trip_assignment)

    cursor.executemany("INSERT INTO employee_assignments (trip_id, employee_id) VALUES (%s, %s)", assignments)
    con.commit()
    print(f"Przypisano pracowników do lotów (łącznie {len(assignments)} przypisań).")
    print("-" * 50)

    # Sekcja 11: Generowanie pasażerów dla lotów
    print("--- ETAP 8: Przypisywanie klientów (pasażerów) do lotów ---")
    cursor.execute('SELECT client_id FROM clients')
    client_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute('SELECT trip_id, spacecraft_id, status, departure_datetime, return_datetime FROM trips')
    trips_data = cursor.fetchall()

    cursor.execute('SELECT spacecraft_id, capacity_passengers FROM spacecraft')
    capacity_map = dict(cursor.fetchall())

    trip_info_passengers = {tid: (capacity_map[sc_id], status, dep, ret) for tid, sc_id, status, dep, ret in trips_data}
    trip_participants = defaultdict(list)
    client_trip_assignments = defaultdict(list)

    filled_trips = [tid for tid, (_, status, _, _) in trip_info_passengers.items() if status in ('completed', 'in progress')]
    available_clients = client_ids.copy()
    random.shuffle(available_clients)

    for trip_id in filled_trips:
        capacity, _, departure, return_dt = trip_info_passengers[trip_id]
        assigned = 0
        for client_id in available_clients:
            if assigned >= capacity: break
            if not is_overlapping(departure, return_dt, client_trip_assignments[client_id]):
                seat_number = len(trip_participants[trip_id]) + 1
                trip_participants[trip_id].append((client_id, seat_number))
                client_trip_assignments[client_id].append((departure, return_dt))
                assigned += 1

    all_trip_ids = list(trip_info_passengers.keys())
    random.shuffle(all_trip_ids)

    for client_id in client_ids:
        if not client_trip_assignments[client_id]:
            for trip_id in all_trip_ids:
                capacity, _, departure, return_dt = trip_info_passengers[trip_id]
                if len(trip_participants[trip_id]) >= capacity: continue
                if not is_overlapping(departure, return_dt, client_trip_assignments[client_id]):
                    seat_number = len(trip_participants[trip_id]) + 1
                    trip_participants[trip_id].append((client_id, seat_number))
                    client_trip_assignments[client_id].append((departure, return_dt))
                    break

    for trip_id, (capacity, status, departure, return_dt) in trip_info_passengers.items():
        if status != 'planned': continue
        remaining_seats = capacity - len(trip_participants[trip_id])
        if remaining_seats <= 0: continue
        random.shuffle(client_ids)
        for client_id in client_ids:
            if remaining_seats == 0: break
            if not is_overlapping(departure, return_dt, client_trip_assignments[client_id]):
                seat_number = len(trip_participants[trip_id]) + 1
                trip_participants[trip_id].append((client_id, seat_number))
                client_trip_assignments[client_id].append((departure, return_dt))
                remaining_seats -= 1

    records_to_insert_participants = [(tid, cid, seat) for tid, parts in trip_participants.items() for cid, seat in parts]
    cursor.executemany("INSERT INTO trip_participants (trip_id, client_id, seat_number) VALUES (%s, %s, %s)", records_to_insert_participants)
    con.commit()
    print(f"Przypisano pasażerów do lotów (łącznie {len(records_to_insert_participants)} rezerwacji).")
    print("-" * 50)
    
    # Sekcja 12: Generowanie kontaktów alarmowych
    print("--- ETAP 9: Generowanie kontaktów alarmowych dla klientów ---")
    cursor.execute('SELECT client_id, date_of_birth FROM clients')
    clients_dob = cursor.fetchall()
    male_relationships = ['ojciec', 'brat', 'mąż', 'partner', 'przyjaciel', 'syn']
    female_relationships = ['matka', 'siostra', 'żona', 'partnerka', 'przyjaciółka', 'córka']
    domains_contacts = ['gmail.com', 'outlook.com', 'yahoo.com', 'protonmail.com']
    records_to_insert_contacts = []
    used_emails_contacts = set()

    for client_id, dob in clients_dob:
        birth_year = dob.year
        if birth_year < 2965: available_relationships = [r for r in male_relationships + female_relationships if r not in ['matka', 'ojciec']]
        elif birth_year > 2990: available_relationships = [r for r in male_relationships + female_relationships if r not in ['syn', 'córka']]
        else: available_relationships = male_relationships + female_relationships
        used_relationships = set()
        for _ in range(2):
            while True:
                gender = random.choice(['male', 'female'])
                first_name = fake.first_name_male() if gender == 'male' else fake.first_name_female()
                last_name = fake.last_name()
                base_email = f"{first_name}.{last_name}".lower().replace(" ", "").replace("'", "")
                domain = random.choice(domains_contacts)
                email = f"{base_email}@{domain}"
                suffix = 1
                while email in used_emails_contacts:
                    email = f"{base_email}{suffix}@{domain}"
                    suffix += 1
                used_emails_contacts.add(email)
                phone = fake.phone_number()
                if random.random() < 0.02: email, phone = None, None
                rel_pool = male_relationships if gender == 'male' else female_relationships
                rel_choices = [r for r in rel_pool if r in available_relationships and r not in used_relationships]
                if rel_choices:
                    relationship = random.choice(rel_choices)
                    used_relationships.add(relationship)
                    break
            records_to_insert_contacts.append((client_id, first_name, last_name, relationship, email, phone))

    cursor.executemany("INSERT INTO emergency_contacts (client_id, first_name, last_name, relationship, email, phone) VALUES (%s, %s, %s, %s, %s, %s)", records_to_insert_contacts)
    con.commit()
    print(f"Wygenerowano {len(records_to_insert_contacts)} kontaktów alarmowych.")
    print("-" * 50)
    
    # Sekcja 13: Generowanie transakcji
    print("--- ETAP 10: Generowanie transakcji finansowych ---")
    cursor.execute("SELECT tp.trip_id, tp.client_id, t.departure_datetime, tt.base_price FROM trip_participants tp JOIN trips t ON tp.trip_id = t.trip_id JOIN trip_types tt ON t.trip_type_id = tt.trip_type_id")
    participants_data = cursor.fetchall()
    payment_methods = ['credit_card', 'wire_transfer', 'paypal', 'crypto']
    future_statuses = ['completed', 'pending']
    records_to_insert_trans = []
    now_trans = datetime.now() + relativedelta(years=1000)
    start_dt = datetime.combine(start_of_business, time(0, 0, 0))

    for trip_id, client_id, departure_dt, base_price in participants_data:
        if departure_dt > now_trans:
            days_diff = (departure_dt - start_dt).days
            max_advance_days = min(180, days_diff) if days_diff > 0 else 1
        else:
            days_diff = (now_trans - start_dt).days
            max_advance_days = min(180, days_diff) if days_diff > 0 else 1
        days_before_departure = random.randint(1, max_advance_days)
        transaction_date = departure_dt - timedelta(days=days_before_departure, hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        if transaction_date < start_dt:
            diff_seconds = int((departure_dt - start_dt).total_seconds())
            random_offset = random.randint(0, diff_seconds)
            transaction_date = start_dt + timedelta(seconds=random_offset)
        if transaction_date > departure_dt:
            # cofamy o losowe 1-24 h
            transaction_date = departure_dt - timedelta(
            seconds=random.randint(3600, 24*3600)
            )
        amount = round(float(base_price) * (1 + random.uniform(-0.05, 0.05)), 2)
        payment_method = random.choice(payment_methods)
        status = random.choices(future_statuses, weights=[0.7, 0.3])[0] if departure_dt > now_trans else 'completed'
        records_to_insert_trans.append((trip_id, client_id, transaction_date, amount, payment_method, status))

    cursor.executemany("INSERT INTO transactions (trip_id, client_id, transaction_date, amount, payment_method, status) VALUES (%s, %s, %s, %s, %s, %s)", records_to_insert_trans)
    con.commit()
    print(f"Wygenerowano {len(records_to_insert_trans)} transakcji.")
    print("-" * 50)
    
    # Sekcja 14: Generowanie incydentów
    print("--- ETAP 11: Generowanie incydentów ---")
    cursor.execute("SELECT trip_id, departure_datetime, return_datetime FROM trips WHERE status = 'completed'")
    trips_data_inc = cursor.fetchall()
    
    cursor.execute("SELECT trip_id, employee_id FROM employee_assignments")
    assignments_inc = cursor.fetchall()
    trip_to_employees = defaultdict(list)
    for trip_id, emp_id in assignments_inc:
        trip_to_employees[trip_id].append(emp_id)
        
    cursor.execute("SELECT trip_id, client_id FROM trip_participants")
    parts_inc = cursor.fetchall()
    trip_to_clients = defaultdict(list)
    for trip_id, client_id in parts_inc:
        trip_to_clients[trip_id].append(client_id)

    incident_templates = [
        {"description": "Problemy z ciśnieniem w module medycznym, szybka reakcja lekarza.", "category": "medical", "requires_client": True, "possible_severities": ["medium", "high"]},
        {"description": "Awaria systemu orientacji – konieczne ręczne sterowanie przez pilota.", "category": "navigation", "requires_client": False, "possible_severities": ["medium", "high", "critical"]},
        {"description": "Utrata łączności z Ziemią na krótki okres, przywrócono po 15 minutach.", "category": "communication", "requires_client": False, "possible_severities": ["low", "medium"]},
        {"description": "Niewielki pożar w komorze silnikowej, ugaszono systemami automatycznymi.", "category": "equipment", "requires_client": False, "possible_severities": ["high", "critical"]},
        {"description": "Podejrzenie choroby lokomocyjnej u jednego z pasażerów, interwencja medyczna.", "category": "medical", "requires_client": True, "possible_severities": ["low", "medium"]},
        {"description": "Nieprawidłowy odczyt czujnika paliwa – wymagana weryfikacja inżynierska.", "category": "equipment", "requires_client": False, "possible_severities": ["medium", "high"]},
        {"description": "Zauważono podejrzany obiekt kosmiczny, wykonano dodatkową analizę.", "category": "navigation", "requires_client": False, "possible_severities": ["low", "medium"]},
        {"description": "Krótki alarm związany z poziomem tlenu, natychmiastowe sprawdzenie.", "category": "security", "requires_client": False, "possible_severities": ["medium", "high"]}
    ]
    incident_rows = []
    now_inc = datetime.now() + relativedelta(years=1000)

    for trip_id, departure_dt, return_dt in trips_data_inc:
        for _ in range(random.randint(0, 2)):
            template = random.choice(incident_templates)
            reported_by = random.choice(trip_to_employees.get(trip_id, [None]))
            involved_client = random.choice(trip_to_clients.get(trip_id, [None])) if template["requires_client"] else None
            end_time = return_dt if return_dt is not None else now_inc
            if end_time > departure_dt:
                total_seconds = int((end_time - departure_dt).total_seconds())
                offset = random.randint(1, total_seconds - 1) if total_seconds > 1 else 1
                incident_time = departure_dt + timedelta(seconds=offset)
            else:
                incident_time = departure_dt + timedelta(hours=1)
            incident_rows.append((trip_id, incident_time, reported_by, involved_client, template["category"], template["description"], random.choice(template["possible_severities"])))
    
    cursor.executemany("INSERT INTO incidents (trip_id, datetime_occurred, reported_by_employee, involved_client_id, category, description, severity) VALUES (%s, %s, %s, %s, %s, %s, %s)", incident_rows)
    con.commit()
    print(f"Wygenerowano {len(incident_rows)} incydentów.")
    print("-" * 50)
    
    # Sekcja 15: Generowanie opinii
    print("--- ETAP 12: Generowanie opinii dla zakończonych lotów ---")
    cursor.execute("""
        SELECT 
            tp.trip_id, 
            tp.client_id, 
            t.return_datetime
        FROM trip_participants tp
        JOIN trips t ON tp.trip_id = t.trip_id
        WHERE t.status = 'completed'
        AND t.return_datetime IS NOT NULL
    """)
    completed_participants = cursor.fetchall()  # lista: (trip_id, client_id, return_datetime)

    # --- 2. Pobierz trip_id, w których były incydenty ---
    cursor.execute("SELECT DISTINCT trip_id FROM incidents")
    trips_with_incidents = {row[0] for row in cursor.fetchall()}

    # --- 3. Listy komentarzy ---
    negative_comments = [
        "Lot był bardzo niewygodny, a obsługa niezbyt pomocna.",
        "Paliwo się skończyło, a procedury awaryjne były chaotyczne.",
        "Posiłki były zimne i bez smaku. Ogólnie rozczarowanie.",
        "Sprzęt naukowy na pokładzie nie działał, stracony czas.",
        "Osoby odpowiedzialne za bezpieczeństwo były niekompetentne.",
        "Kabina zbyt mała, bardzo ciasno i duszno.",
        "Trudności z łącznością, nie mogłem porozmawiać z rodziną."
    ]
    neutral_comments = [
        "Lot odbył się zgodnie z planem, ale niczym szczególnym się nie wyróżniał.",
        "Stacja na orbicie spełniła minimalne oczekiwania, trudno coś więcej dodać.",
        "Czas spędzony w stanie nieważkości był interesujący, ale krótki.",
        "Obsługa była w miarę profesjonalna, choć bez entuzjazmu.",
        "Warunki codziennego pobytu w module były przeciętne."
    ]
    positive_comments = [
        "Przelot przebiegł bez zarzutu, widoki zapierające dech w piersiach.",
        "Personel bardzo pomocny, poczułem się w pełni bezpiecznie.",
        "Eksperymenty naukowe w module okazały się fascynujące.",
        "Kabina komfortowa, z dużą przestrzenią i świetnym widokiem.",
        "Powrót na Ziemię był płynny, lądowanie perfekcyjne.",
        "Panel widokowy statku doskonale zaprojektowany dla fotografów.",
        "Program edukacyjny na pokładzie dostarczył wiele wiedzy."
    ]

    # --- 4. Generowanie i wstawianie opinii ---
    feedback_rows = []
    now = datetime.now() + relativedelta(years=1000)

    for trip_id, client_id, return_dt in completed_participants:
        # ok. 70% zostawia opinię
        if random.random() > 0.7:
            continue

        if trip_id in trips_with_incidents:
            rating = random.choices([1,2,3,4,5], weights=[0.1, 0.25, 0.3, 0.2, 0.15])[0]
        else:
            rating = random.choices([1,2,3,4,5], weights=[0.05, 0.1, 0.15, 0.3, 0.4])[0]

        # dobór komentarza do oceny
        if rating <= 2:
            comments = random.choice(negative_comments)
        elif rating == 3:
            comments = random.choice(neutral_comments)
        else:
            comments = random.choice(positive_comments)

        # data przesłania: między 1 a 30 dni po return_dt, ale nie później niż teraz
        raw_date = return_dt + timedelta(
            days=random.randint(1,30),
            hours=random.randint(0,23),
            minutes=random.randint(0,59),
            seconds=random.randint(0,59)
        )
        submitted_at = raw_date if raw_date < now else now - timedelta(
            days=random.randint(0,3),
            hours=random.randint(0,23),
            minutes=random.randint(0,59),
            seconds=random.randint(0,59)
        )

        feedback_rows.append((trip_id, client_id, rating, comments, submitted_at))

    cursor.executemany(
        """
        INSERT INTO feedback (
            trip_id, client_id, rating, comments, submitted_at
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        feedback_rows
    )
    con.commit()
    
    print(f"Wygenerowano {len(feedback_rows)} opinii.")
    print("-" * 50)
    # Sekcja 16: Generowanie kosztów
    print("--- ETAP 13: Generowanie kosztów dla każdej wyprawy ---")
    cursor.execute("SELECT t.trip_id, tt.base_price FROM trips t JOIN trip_types tt ON t.trip_type_id = tt.trip_type_id")
    trip_base_prices = cursor.fetchall()

    base_cost_templates = [
        {"description": "paliwo", "min_pct": 0.15, "max_pct": 0.25}, {"description": "obsługa załogi", "min_pct": 0.10, "max_pct": 0.18},
        {"description": "zapasy żywności", "min_pct": 0.05, "max_pct": 0.10}, {"description": "utrzymanie statku", "min_pct": 0.08, "max_pct": 0.12},
        {"description": "ubezpieczenie", "min_pct": 0.03, "max_pct": 0.06}, {"description": "telekomunikacja", "min_pct": 0.02, "max_pct": 0.04},
        {"description": "amortyzacja sprzętu", "min_pct": 0.05, "max_pct": 0.10}, {"description": "usługi medyczne", "min_pct": 0.02, "max_pct": 0.05},
        {"description": "utrzymanie stacji startowej", "min_pct": 0.04, "max_pct": 0.08}, {"description": "serwis i naprawy", "min_pct": 0.06, "max_pct": 0.10}
    ]
    incident_cost_map = {
        "equipment": {"description": "dodatkowa naprawa sprzętu (z incydentu)", "min_pct": 0.05, "max_pct": 0.10},
        "medical": {"description": "dodatkowe usługi medyczne (z incydentu)", "min_pct": 0.03, "max_pct": 0.07},
        "security": {"description": "operacje bezpieczeństwa (z incydentu)", "min_pct": 0.005, "max_pct": 0.01},
        "navigation": {"description": "korekta kursu (z incydentu)", "min_pct": 0.005, "max_pct": 0.1},
        "communication": {"description": "przywrócenie łączności (z incydentu)", "min_pct": 0.02, "max_pct": 0.06},
    }

    cursor.execute("SELECT trip_id, category FROM incidents")
    incidents_data = cursor.fetchall()
    trip_to_incidents = defaultdict(set)
    for trip_id, category in incidents_data:
        trip_to_incidents[trip_id].add(category)

    cost_rows = []
    for trip_id, base_price in trip_base_prices:
        bp = float(base_price)
        for tpl in base_cost_templates:
            cost_rows.append((trip_id, tpl["description"], round(bp * random.uniform(tpl["min_pct"], tpl["max_pct"]), 2)))
        for cat in trip_to_incidents.get(trip_id, []):
            if cat in incident_cost_map:
                tpl_inc = incident_cost_map[cat]
                cost_rows.append((trip_id, tpl_inc["description"], round(bp * random.uniform(tpl_inc["min_pct"], tpl_inc["max_pct"]), 2)))
    
    cursor.executemany("INSERT INTO costs (trip_id, description, cost_amount) VALUES (%s, %s, %s)", cost_rows)
    con.commit()
    print(f"Wygenerowano {len(cost_rows)} wpisów kosztowych.")
    print("-" * 50)
    
    # Zakończenie pracy
    print("--- ZAKOŃCZONO ---")
    print("Populacja bazy danych zakończona pomyślnie.")
    cursor.close()
    con.close()
    print("Połączenie z bazą danych zostało zamknięte.")


if __name__ == '__main__':
    uruchom()