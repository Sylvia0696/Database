import pymysql.cursors
import json
import uuid

pymysql_exceptions = (
    pymysql.err.IntegrityError,
    pymysql.err.MySQLError,
    pymysql.err.ProgrammingError,
    pymysql.err.InternalError,
    pymysql.err.DatabaseError,
    pymysql.err.DataError,
    pymysql.err.InterfaceError,
    pymysql.err.NotSupportedError,
    pymysql.err.OperationalError)

params = {
    "dbhost": "localhost",                    # Changeable defaults in constructor
    "port": 3306,
    "dbname": "classiccars",
    "dbuser": "dbuser",
    "dbpw": "dbuser",
    "cursorClass": pymysql.cursors.DictCursor,        # Default setting for DB connections
    "charset":  'utf8mb4'                             # Do not change
}


def get_new_connection(params=params):
    cnx = pymysql.connect(
        host=params["dbhost"],
        port=params["port"],
        user=params["dbuser"],
        password=params["dbpw"],
        db=params["dbname"],
        charset=params["charset"],
        cursorclass=params["cursorClass"])
    return cnx


def run_q(cnx, q, args, fetch=False, commit=True, cursor=None):
    """
    :param cnx: The database connection to use.
    :param q: The query string to run.
    :param args: Parameters to insert into query template if q is a template.
    :param fetch: True if this query produces a result and the function should perform and return fetchall()
    :return:
    """
    #debug_message("run_q: q = " + q)
    #ut.debug_message("Q = " + q)
    #ut.debug_message("Args = ", args)

    result = None

    try:
        if cursor is None:
            cnx = get_new_connection()
            cursor = cnx.cursor()

        result = cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
        if commit:
            cnx.commit()
    except pymysql_exceptions as original_e:
        #print("dffutils.run_q got exception = ", original_e)
        raise(original_e)

    return result


def create_account(balance):
    params['dbname'] = "w4111final"
    cnx = get_new_connection(params)
    cur = cnx.cursor()
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    
    version_id = str(uuid.uuid4())
    q = "insert into banking_account (balance, version) values (%s, %s)"
    result = run_q(cnx, q, (balance, version_id), True, commit = True)
    
    q2 = "select max(id) as new_id from banking_account;"
    result2 = run_q(cnx, q2, None, fetch = True, commit = False)
    
    result2 = result2[0]['new_id']
    cnx.commit()
    cnx.close()
    
    return result2


def get_balance(id, cursor=None):
    """
    Same logic as above. Normally, there would be a single function that returned data based on
    requested fields instead of two different functions.
    """
    
    if cursor is None:
        params['dbname'] = "w4111final"
        cnx = get_new_connection(params)
        cur = cnx.cursor()
        cursor_created = True
    else:
        cursor_created = False
        cnx = None

    q = "select * from banking_account where id=%s"
    result = run_q(cnx, q, id, fetch=True, commit=False)

    if cursor_created:
        cnx.commit()
        cnx.close()

    return result[0]['balance']



def get_account(id, cursor=None):
    """
    Same logic as above. Normally, there would be a single function that returned data based on
    requested fields instead of two different functions.
    """
    
    if cursor is None:
        params['dbname'] = "w4111final"
        cnx = get_new_connection(params)
        cur = cnx.cursor()
        cursor_created = True
    else:
        cursor_created = False
        cnx = None

    q = "select * from banking_account where id=%s"
    result = run_q(cnx, q, id, fetch=True, commit=False)

    if cursor_created:
        cnx.commit()
        cnx.close()

    return result[0]


def update_balance(id, amount, cursor = None):
    cnx = None
    if cursor is None:
        params['dbname'] = "w4111final"
        cnx = get_new_connection(params)
        cur = cnx.cursor()
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        cursor_created = True
    else:
        cursor_created = False
        
    new_version = str(uuid.uuid4())
    q = "update banking_account set balance = %s, version = %s where id = %s"
    result = run_q(cnx, q, (amount, new_version, id), fetch = True, commit = True, cursor = cursor)
    
    if cursor_created:
        cnx.commit()
        cnx.close()



def update_balance_optimistic(acct, amount, cursor = None):
    cnx = None
    if cursor is None:
        params['dbname'] = "w4111final"
        cnx = get_new_connection(params)
        cur = cnx.cursor()
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        cursor_created = True
    else:
        cursor_created = False
        
    current_account = get_account(acct['id'])
    if current_account['version'] != acct['version']:
        raise ValueError("Optimistic transaction failed")
        
    new_version = str(uuid.uuid4())
    q = "update banking_account set balance = %s, version = %s where id = %s"
    result = run_q(cnx, q, (amount, new_version, acct['id']), fetch = True, commit = True, cursor = cursor)
    
    if cursor_created:
        cnx.commit()
        cnx.close()



def transfer_pessmistic():
    print("\n*** Transfering Pessmisticlly ***\n")
    params['dbname'] = "w4111final"
    cnx = get_new_connection(params)
    cur = cnx.cursor()
    
    try:
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        source_id = input("Source account id: ")
        source_b = get_balance(source_id, cursor = cur)
        cont = input("Source Balance = " + str(source_b) + ". Continue(y/n)")
        
        if cont == "y":
            target_id = input("Target account id: ")
            target_b = get_balance(target_id, cursor = cur)
            cont = input("Target balance = " + str(target_b) + ". Continue(y/n)")
            
            if cont == "y":
                amount = input("Amount: ")
                amount = float(amount)
                new_source = source_b - amount
                new_target = target_b + amount
                
                update_balance(source_id, new_source, cursor = cur)
                update_balance(target_id, new_target, cursor = cur)
                
                cnx.commit()
                cnx.close()
    except Exception as e:
        print("Exception =", e)
        cnx.rollback()
        cnx.close()
        
    return



def transfer_optimistic():
    print("\n*** Transfering optimisticlly ***\n")

    source_id = input("Source account id: ")
    source_acct = get_account(source_id, cursor = None)

    cont = input("Source Balance = " + str(source_acct['balance']) + ". Continue(y/n)")

    if cont == "y":
        target_id = input("Target account id: ")
        target_acct = get_account(target_id, cursor = None)
        cont = input("Target balance = " + str(target_acct['balance']) + ". Continue(y/n)")

        if cont == "y":
            amount = input("Amount: ")
            amount = float(amount)
            new_source = source_acct['balance'] - amount
            new_target = target_acct['balance'] + amount
            
            try:
                params['dbname'] = "w4111final"
                cnx = get_new_connection(params)
                cur = cnx.cursor()
                cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                update_balance_optimistic(source_acct, new_source, cursor = cur)
                update_balance_optimistic(target_acct, new_target, cursor = cur)

                cnx.commit()
                cnx.close()
            except Exception as e:
                print("Exception =", e)
                cnx.rollback()
                cnx.close()
        
    return


if __name__ == "__main__":
    transfer_optimistic()