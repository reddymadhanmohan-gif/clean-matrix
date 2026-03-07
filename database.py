import os
import hashlib

try:
    import streamlit as st
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')

from supabase import create_client, Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def signup_user(username: str, password: str) -> dict:
    try:
        existing = supabase.table('users').select('id').eq('username', username).execute()
        if existing.data:
            return {'success': False, 'message': 'Username already taken!'}
        hashed = hash_password(password)
        result = supabase.table('users').insert({
            'username': username,
            'password': hashed
        }).execute()
        return {'success': True, 'message': 'Account created!', 'user': result.data[0]}
    except Exception as e:
        return {'success': False, 'message': f'Error: {str(e)}'}

def login_user(username: str, password: str) -> dict:
    try:
        hashed = hash_password(password)
        result = supabase.table('users').select('*').eq('username', username).eq('password', hashed).execute()
        if result.data:
            return {'success': True, 'message': 'Login successful!', 'user': result.data[0]}
        else:
            return {'success': False, 'message': 'Invalid username or password.'}
    except Exception as e:
        return {'success': False, 'message': f'Error: {str(e)}'}

def save_dataset_record(user_id, filename, row_count, col_count):
    try:
        result = supabase.table('datasets').insert({
            'user_id': user_id,
            'filename': filename,
            'row_count': row_count,
            'col_count': col_count,
            'status': 'uploaded'
        }).execute()
        return result.data[0]['id']
    except Exception as e:
        print(f'Error saving dataset record: {e}')
        return None

def save_processed_record(dataset_id, user_id, orig_file, clean_file, missing_vals, outliers):
    try:
        supabase.table('processed_files').insert({
            'dataset_id': dataset_id,
            'user_id': user_id,
            'original_filename': orig_file,
            'cleaned_filename': clean_file,
            'missing_values': missing_vals,
            'outliers_removed': outliers
        }).execute()
    except Exception as e:
        print(f'Error saving processed record: {e}')
```