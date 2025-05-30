#!/usr/bin/env python3
"""
Setup script to create secrets directory and files safely.
"""

import os
import getpass
import stat

def setup_secrets(secrets_dir="secrets"):
    """Create secrets directory and credential files"""
    
    # Create secrets directory if it doesn't exist
    if not os.path.exists(secrets_dir):
        os.makedirs(secrets_dir)
        print(f"✅ Created {secrets_dir}/ directory")
    
    # Set directory permissions (owner read/write/execute only)
    os.chmod(secrets_dir, stat.S_IRWXU)
    
    # Get credentials from user
    print("\nEnter your database credentials:")
    db_user = input("Database username: ").strip()
    db_password = getpass.getpass("Database password: ").strip()
    
    if not db_user or not db_password:
        print("❌ Username and password cannot be empty")
        return False
    
    # Write credential files
    user_file = os.path.join(secrets_dir, "db_user.txt")
    password_file = os.path.join(secrets_dir, "db_password.txt")
    
    try:
        # Write username
        with open(user_file, 'w') as f:
            f.write(db_user)
        os.chmod(user_file, stat.S_IRUSR | stat.S_IWUSR)  # Owner read/write only
        
        # Write password
        with open(password_file, 'w') as f:
            f.write(db_password)
        os.chmod(password_file, stat.S_IRUSR | stat.S_IWUSR)  # Owner read/write only
        
        print(f"✅ Created {user_file}")
        print(f"✅ Created {password_file}")
        print(f"✅ Set secure file permissions (600)")
        print(f"\n🔒 Your credentials are now stored securely!")
        print(f"🚨 Remember to add '{secrets_dir}/' to your .gitignore file!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating credential files: {e}")
        return False

def create_gitignore_entry(secrets_dir="secrets"):
    """Add secrets directory to .gitignore if it exists"""
    gitignore_path = ".gitignore"
    secrets_entry = f"{secrets_dir}/"
    
    # Check if .gitignore exists
    if os.path.exists(gitignore_path):
        # Read existing content
        with open(gitignore_path, 'r') as f:
            content = f.read()
        
        # Check if secrets directory is already ignored
        if secrets_entry not in content:
            # Append to .gitignore
            with open(gitignore_path, 'a') as f:
                if not content.endswith('\n'):
                    f.write('\n')
                f.write(f"# Database credentials\n{secrets_entry}\n")
            print(f"✅ Added '{secrets_entry}' to .gitignore")
        else:
            print(f"✅ '{secrets_entry}' already in .gitignore")
    else:
        # Create new .gitignore
        with open(gitignore_path, 'w') as f:
            f.write(f"# Database credentials\n{secrets_entry}\n")
        print(f"✅ Created .gitignore with '{secrets_entry}'")

if __name__ == "__main__":
    print("🔐 Database Credentials Setup")
    print("=" * 40)
    
    if setup_secrets():
        create_gitignore_entry()
        print("\n✨ Setup complete! You can now run the export tool without --user and --password flags.")
    else:
        print("\n❌ Setup failed. Please try again.")
