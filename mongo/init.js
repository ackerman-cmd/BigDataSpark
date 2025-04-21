db = db.getSiblingDB('sales_db');
db.createCollection('init');
db.createUser({
    user: 'admin',
    pwd: 'secret',
    roles: [
        { role: 'readWrite', db: 'sales_db' },
        { role: 'dbAdmin', db: 'sales_db' }
    ]
});