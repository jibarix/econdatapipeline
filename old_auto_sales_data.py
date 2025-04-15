from models import get_supabase_client

def insert_auto_sales_data():
    # Get Supabase client
    supabase = get_supabase_client()
    
    # Data to insert
    sales_data = [
        {'date': '2014-01-01', 'sales': 7932},
        {'date': '2014-02-01', 'sales': 7904},
        {'date': '2014-03-01', 'sales': 8262},
        {'date': '2014-04-01', 'sales': 7566},
        {'date': '2014-05-01', 'sales': 7736},
        {'date': '2014-06-01', 'sales': 9279},
        {'date': '2014-07-01', 'sales': 6328},
        {'date': '2014-08-01', 'sales': 7027},
        {'date': '2014-09-01', 'sales': 6672},
        {'date': '2014-10-01', 'sales': 6499},
        {'date': '2014-11-01', 'sales': 7381},
        {'date': '2014-12-01', 'sales': 9533},
        {'date': '2015-01-01', 'sales': 6354},
        {'date': '2015-02-01', 'sales': 6430},
        {'date': '2015-03-01', 'sales': 7079},
        {'date': '2015-04-01', 'sales': 5616},
        {'date': '2015-05-01', 'sales': 6546},
        {'date': '2015-06-01', 'sales': 7685}
    ]
    
    # Insert data
    # Option 1: Basic insert (will error if records already exist)
    result = supabase.table('auto_sales').insert(sales_data).execute()
    
    # Option 2: Use upsert to handle duplicates (updates existing records)
    # result = supabase.table('auto_sales').upsert(sales_data).execute()
    
    print(f"Inserted {len(sales_data)} records into auto_sales table")
    return result

if __name__ == "__main__":
    insert_auto_sales_data()