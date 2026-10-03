"""
Incremental Dirty Data Generator
=================================
Run 1 (no state file yet): generates the FULL "master" tables
(departments, employees, categories, products, suppliers,
product_suppliers, customers, shippers) once, plus an initial batch
of orders/order_details/payments/shipments.

Every next run: master tables are NOT regenerated (they're stable
reference data). Only a NEW small batch of orders/order_details/
payments/shipments is generated, with IDs continuing from where the
last run left off -- simulating "new activity since last time".

Each run writes its own timestamped file into raw_data/<table>/,
e.g. raw_data/orders/orders_20260927_140000.csv, so Auto Loader
picks up only the new file next time it runs.

Scheduling "every 5 hours" is NOT done inside this script -- schedule
this script itself to run every 5 hours via a Databricks Workflow Job
(or cron if running locally). This script just does ONE generation
pass per call.
"""

import csv
import json
import random
import os
from datetime import date, timedelta, datetime
from faker import Faker

fake = Faker('en_US')

RAW_DIR = "/Volumes/workspace/default/ecommerce_medallion_pipeline/raw_data"
STATE_FILE = "/Volumes/workspace/default/ecommerce_medallion_pipeline/generator_state.json"

# Master-table sizes (only used on the very first run)
NUM_CUSTOMERS = 10000
NUM_CATEGORIES = 20
NUM_PRODUCTS = 1000
NUM_DEPARTMENTS = 10
NUM_EMPLOYEES = 200
NUM_SUPPLIERS = 100
NUM_SHIPPERS = 10

# How much NEW activity to generate per run (tune to taste)
NEW_ORDERS_PER_RUN = 2000
PAYMENT_COVERAGE = 0.80   # ~80% of new orders get a payment record
SHIPMENT_COVERAGE = 0.80  # ~80% of new eligible orders get a shipment record

NULL_TOKENS = ["", "NULL", "null", "N/A", "n/a", "None", "-", "NaN"]

def maybe_null(value, p=0.06):
    return random.choice(NULL_TOKENS) if random.random() < p else value

def random_date_format(d):
    if d is None:
        return random.choice(NULL_TOKENS)
    fmt = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d %b %Y", "%Y/%m/%d", "%B %d, %Y"])
    return d.strftime(fmt)

def dirty_number(value, allow_text=True):
    roll = random.random()
    if roll < 0.03:
        return f"${value:,.2f}"
    elif roll < 0.06:
        return f"{value:,.2f}"
    elif roll < 0.08 and allow_text:
        return "unknown"
    elif roll < 0.10:
        return f" {value} "
    return value

def write_table_csv(table_name, headers, rows, delimiter=","):
    """Writes a timestamped file into raw_data/<table_name>/ so Auto Loader
    sees a brand-new file each run instead of overwriting one file."""
    folder = os.path.join(RAW_DIR, table_name)
    os.makedirs(folder, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(folder, f"{table_name}_{ts}.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Wrote {filepath} ({len(rows)} rows)")
    return filepath

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return None

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

BASE_DEPARTMENTS = ["Sales", "IT", "HR", "Finance", "Logistics", "Marketing",
                     "Customer Service", "Legal", "R&D", "Procurement"]
BASE_SHIPPERS = ["DHL", "FedEx", "UPS", "Aramex", "USPS", "Amazon Logistics",
                  "TNT", "China Post", "Royal Mail", "SF Express"]
BASE_CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Sports", "Beauty",
                    "Books", "Toys", "Automotive", "Garden", "Office Supplies",
                    "Pet Supplies", "Health", "Jewelry", "Shoes", "Baby Products",
                    "Music", "Movies", "Groceries", "Furniture", "Tools"]
CATEGORY_SUFFIXES = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Monitor", "Smartwatch", "Headphones", "Camera"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Sweater", "Hoodie", "Shorts"],
    "Home & Kitchen": ["Blender", "Microwave", "Coffee Maker", "Toaster", "Vacuum Cleaner", "Kettle"],
    "Sports": ["Racket", "Yoga Mat", "Dumbbells", "Football", "Treadmill", "Bicycle"],
    "Beauty": ["Serum", "Shampoo", "Moisturizer", "Sunscreen", "Perfume", "Lipstick"],
}
GENERIC_SUFFIXES = ["Pro", "Max", "Lite", "Plus", "Ultra", "Kit", "Set", "Bundle"]
BRANDS = ["Samsung", "Apple", "Sony", "Nike", "Adidas", "Bosch", "Philips", "L'Oreal", "Generic Co"]
MALE_VARS = ["Male", "male", "M", "m", "1", " Male ", "MALE"]
FEMALE_VARS = ["Female", "female", "F", "f", "0", " Female ", "FEMALE"]
PAYMENT_METHODS_VARS = ["Cash", "cash", "Cash on Delivery", "cash on delevry", "COD", "cod", "C.O.D",
                         "Credit Card", "credit card", "Visa", "MasterCard", "CC", "PayPal", "paypal", "Bank Transfer"]
ORDER_STATUS_VARS_DELIVERED = ["Delivered", "delivered", "DELIVERED", " Delivered "]
ORDER_STATUS_VARS_SHIPPED = ["Shipped", "shipped", "SHIPPED"]
ORDER_STATUS_VARS_PENDING = ["Pending", "pending", " PENDING "]
ORDER_STATUS_VARS_CANCELLED = ["Cancelled", "canceled", "Canceled", "CANCELLED"]
PAYMENT_STATUS_VARS_COMPLETED = ["Completed", "completed", "Done", "Success"]
PAYMENT_STATUS_VARS_FAILED = ["Failed", "failed", "Error", "Declined"]

def build_names(base_list, target_count, prefix):
    if target_count <= len(base_list):
        return base_list[:target_count]
    return base_list + [f"{prefix} {i}" for i in range(len(base_list) + 1, target_count + 1)]


def generate_master_tables():
    """Runs ONCE. Generates all stable reference tables + customer pool."""
    departments = [[i + 1, name] for i, name in enumerate(build_names(BASE_DEPARTMENTS, NUM_DEPARTMENTS, "Department"))]
    write_table_csv("departments", ["DepartmentID", "DepartmentName"], departments)

    shippers = [[i + 1, name] for i, name in enumerate(build_names(BASE_SHIPPERS, NUM_SHIPPERS, "Shipper"))]
    write_table_csv("shippers", ["ShipperID", "CompanyName"], shippers)

    categories_list = build_names(BASE_CATEGORIES, NUM_CATEGORIES, "Category")
    categories = [[i + 1, name] for i, name in enumerate(categories_list)]
    write_table_csv("categories", ["CategoryID", "CategoryName"], categories)

    dept_members = {i + 1: [] for i in range(NUM_DEPARTMENTS)}
    employees = []
    sales_employee_ids = []
    for emp_id in range(1, NUM_EMPLOYEES + 1):
        dept_id = random.randint(1, NUM_DEPARTMENTS)
        if dept_id == 1:
            sales_employee_ids.append(emp_id)
        manager_id = random.choice(dept_members[dept_id]) if dept_members[dept_id] else None
        if random.random() < 0.05:
            manager_id = NUM_EMPLOYEES + random.randint(1, 5)
        is_male = random.choice([True, False])
        first_name = fake.first_name_male() if is_male else fake.first_name_female()
        last_name = fake.last_name()
        gender_val = random.choice(MALE_VARS) if is_male else random.choice(FEMALE_VARS)
        hire_date = fake.date_between(start_date='-5y', end_date='-1y')
        salary = round(random.uniform(4000, 15000), 2)
        employees.append([emp_id, maybe_null(manager_id), dept_id, first_name, last_name,
                           gender_val, dirty_number(salary), random_date_format(hire_date)])
        dept_members[dept_id].append(emp_id)
    if not sales_employee_ids:
        sales_employee_ids = [1]
    write_table_csv("employees", ["EmployeeID", "ManagerID", "DepartmentID", "FirstName", "LastName",
                                    "Gender", "Salary", "HireDate"], employees)

    products = []
    for prod_id in range(1, NUM_PRODUCTS + 1):
        cat_idx = (prod_id - 1) % NUM_CATEGORIES
        cat_name = categories_list[cat_idx]
        suffixes = CATEGORY_SUFFIXES.get(cat_name, GENERIC_SUFFIXES)
        p_name = f"{fake.word().capitalize()} {random.choice(suffixes)}"
        price = round(random.uniform(20, 1500), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)
        brand = random.choice(BRANDS)
        stock = random.randint(0, 300)
        attributes = json.dumps({"warranty_months": random.choice([0, 6, 12, 24]),
                                  "color": random.choice(["black", "white", "silver", None])})
        products.append([prod_id, cat_idx + 1, p_name, brand, dirty_number(price),
                          dirty_number(cost, allow_text=False), maybe_null(stock), attributes])
    write_table_csv("products", ["ProductID", "CategoryID", "ProductName", "Brand", "Price",
                                   "Cost", "Stock", "Attributes"], products)

    suppliers = [[i + 1, f"{fake.company()} Supplies", fake.country()] for i in range(NUM_SUPPLIERS)]
    write_table_csv("suppliers", ["SupplierID", "SupplierName", "Country"], suppliers)

    product_suppliers = []
    max_link = min(3, NUM_SUPPLIERS)
    for p in products:
        for sid in random.sample(range(1, NUM_SUPPLIERS + 1), random.randint(1, max_link)):
            product_suppliers.append([p[0], sid])
    write_table_csv("product_suppliers", ["ProductID", "SupplierID"], product_suppliers)

    customers = []
    customer_reg_dates = {}
    for cust_id in range(1, NUM_CUSTOMERS + 1):
        reg_date = fake.date_between(start_date='-4y', end_date='today')
        customer_reg_dates[cust_id] = reg_date.isoformat()
        is_male = random.choice([True, False])
        first_name = fake.first_name_male() if is_male else fake.first_name_female()
        last_name = fake.last_name()
        gender_val = random.choice(MALE_VARS) if is_male else random.choice(FEMALE_VARS)
        email = f"{first_name.lower()}.{last_name.lower()}{cust_id}@example.com"
        city = fake.city()
        customers.append([cust_id, first_name, last_name, gender_val, maybe_null(email),
                           city, fake.country(), random_date_format(reg_date)])
    write_table_csv("customers", ["CustomerID", "FirstName", "LastName", "Gender", "Email",
                                    "City", "Country", "RegistrationDate"], customers, delimiter=";")

    return {
        "next_order_id": 1,
        "next_order_detail_id": 1,
        "next_payment_id": 1,
        "next_shipment_id": 1,
        "num_customers": NUM_CUSTOMERS,
        "sales_employee_ids": sales_employee_ids,
        "num_products": NUM_PRODUCTS,
        "num_shippers": NUM_SHIPPERS,
        "customer_reg_dates": customer_reg_dates,
    }


def generate_new_activity_batch(state):
    """Runs EVERY time. Generates only NEW orders + related rows,
    continuing IDs from the last run."""
    start_order_id = state["next_order_id"]
    order_ids_this_run = range(start_order_id, start_order_id + NEW_ORDERS_PER_RUN)

    orders, order_details = [], []
    order_meta = {}
    detail_id = state["next_order_detail_id"]

    for order_id in order_ids_this_run:
        cust_id = random.randint(1, state["num_customers"])
        if random.random() < 0.03:
            cust_id = state["num_customers"] + random.randint(1, 10)  # orphan FK

        sales_emp = random.choice(state["sales_employee_ids"])
        reg_date_str = state["customer_reg_dates"].get(str(cust_id))
        order_date = fake.date_between(start_date='-3y', end_date='today')

        base_status = random.choices(["Delivered", "Shipped", "Pending", "Cancelled"], weights=[50, 30, 15, 5])[0]
        status = random.choice({
            "Delivered": ORDER_STATUS_VARS_DELIVERED, "Shipped": ORDER_STATUS_VARS_SHIPPED,
            "Cancelled": ORDER_STATUS_VARS_CANCELLED, "Pending": ORDER_STATUS_VARS_PENDING,
        }[base_status])

        orders.append([order_id, maybe_null(cust_id), sales_emp, random_date_format(order_date), status])
        order_meta[order_id] = (order_date, base_status)

        for _ in range(random.randint(1, 4)):
            prod_id = random.randint(1, state["num_products"])
            qty = random.randint(1, 3)
            unit_price = round(random.uniform(20, 1500), 2)
            discount = random.choice([0, 0, 5, 10, 15, 0.05])
            order_details.append([detail_id, order_id, prod_id, qty, unit_price, discount])
            detail_id += 1

    write_table_csv("orders", ["OrderID", "CustomerID", "SalesEmployeeID", "OrderDate", "Status"], orders)
    write_table_csv("order_details", ["OrderDetailID", "OrderID", "ProductID", "Quantity", "UnitPrice", "Discount"], order_details)

    # payments: ~80% coverage of this run's new orders
    payments = []
    payment_id = state["next_payment_id"]
    payment_order_ids = random.sample(list(order_meta.keys()), int(len(order_meta) * PAYMENT_COVERAGE))
    for oid in payment_order_ids:
        order_date, base_status = order_meta[oid]
        payment_date = order_date + timedelta(days=random.randint(0, 2))
        pay_status = (random.choice(PAYMENT_STATUS_VARS_COMPLETED) if base_status in ["Delivered", "Shipped"]
                      else random.choice(PAYMENT_STATUS_VARS_COMPLETED + PAYMENT_STATUS_VARS_FAILED))
        amount = round(random.uniform(20, 4000), 2)
        payments.append([payment_id, oid, random.choice(PAYMENT_METHODS_VARS), random_date_format(payment_date),
                          dirty_number(amount, allow_text=False), pay_status])
        payment_id += 1
    write_table_csv("payments", ["PaymentID", "OrderID", "PaymentMethod", "PaymentDate", "Amount", "PaymentStatus"], payments)

    # shipments: ~80% coverage of this run's new non-cancelled orders
    shipments = []
    shipment_id = state["next_shipment_id"]
    eligible = [oid for oid, (_, status) in order_meta.items() if status != "Cancelled"]
    shipment_order_ids = random.sample(eligible, int(len(eligible) * SHIPMENT_COVERAGE)) if eligible else []
    for oid in shipment_order_ids:
        order_date, base_status = order_meta[oid]
        ship_date = order_date + timedelta(days=random.randint(1, 3))
        delivery_date = ship_date + timedelta(days=random.randint(1, 6)) if base_status == "Delivered" else None
        shipper_id = random.randint(1, state["num_shippers"])
        shipments.append([shipment_id, oid, shipper_id, random_date_format(ship_date), random_date_format(delivery_date)])
        shipment_id += 1
    write_table_csv("shipments", ["ShipmentID", "OrderID", "ShipperID", "ShipDate", "DeliveryDate"], shipments)

    # CDC-like file: a few late-arriving status updates for recent orders
    cdc_rows = []
    for _ in range(max(5, len(order_meta) // 100)):
        oid = random.choice(list(order_meta.keys()))
        new_status = random.choice(
            ORDER_STATUS_VARS_DELIVERED + ORDER_STATUS_VARS_CANCELLED + ORDER_STATUS_VARS_SHIPPED
        )
        event_ts = datetime.now() - timedelta(hours=random.randint(1, 5))
        cdc_rows.append([oid, new_status, event_ts.isoformat(), random.choice(["UPDATE", "INSERT"])])
    write_table_csv("orders_cdc_incremental", ["OrderID", "Status", "EventTimestamp", "Operation"], cdc_rows)

    # Update state for next run
    state["next_order_id"] = start_order_id + NEW_ORDERS_PER_RUN
    state["next_order_detail_id"] = detail_id
    state["next_payment_id"] = payment_id
    state["next_shipment_id"] = shipment_id
    return state


if __name__ == "__main__":
    random.seed()  # do NOT fix the seed here -- each scheduled run should differ
    state = load_state()
    if state is None:
        print("No state file found -> first run: generating full master tables + initial batch.")
        state = generate_master_tables()
    else:
        print("State file found -> incremental run: generating only new activity.")

    state = generate_new_activity_batch(state)
    save_state(state)
    print(f"\nDone. Next run will start at OrderID {state['next_order_id']}.")