// Runs the bot using mysql to store any
// settings and channel links.
//
// This requires:
// the DATABASE_URL environment variable to be set.

var Facebot = require('../lib/facebot');
var mysql = require('mysql');

var envVars = [
    'BOT_API_KEY',
    'FACEBOOK_EMAIL',
    'FACEBOOK_PASSWORD',
    'AUTHORISED_USERNAME',
    'DATABASE_URL',
    'METADATA_TRACKING',
];

envVars.forEach(function(name) {
    if (process.env[name] == null)
        throw new Error('Environment Variable ' + name + ' not set');
});
var CREATE_SETTINGS_SQL = "CREATE TABLE settings ( id INT, settings_json TEXT, PRIMARY KEY(id) );";
var CREATE_MESSAGES_SQL = "CREATE TABLE messages( fb_threadid VARCHAR(80), fb_message_id VARCHAR(80), slack_message_timestamp VARCHAR(80), fb_message_timestamp BIGINT, is_cur_read BOOL, PRIMARY KEY(fb_message_id), INDEX(fb_threadid) );";
var pool;
function create_pool(callback){
    var settings = JSON.parse(process.env.DATABASE_URL);
    settings.supportBigNumbers=true;
    pool= mysql.createPool(settings);
    pool.getConnection(function(err, client) {
        if(err){
            return callback(new Error("Couldn't connect to mysql db: " + err.message));
        }
   
    createTableIfNeeded(client,"settings",CREATE_SETTINGS_SQL, function (err){
            if(err){
                client.release();
                return callback(new Error("Couldn't create the settings table: " + err.message));  
            }
            createTableIfNeeded(client,"messages",CREATE_MESSAGES_SQL, function (err){
            if(err){
                client.release();
                return callback(new Error("Couldn't create the messages table: " + err.message));  
            }
            });
            return callback(null);
    });
    });
}
//msg_obj should have the props with the column names
function insert_message(msg_obj,callback) {
    pool.getConnection(function(err, client) {
        if(err)
            return callback(new Error("Couldn't connect to mysql db: " + err.message));
        client.query("INSERT IGNORE INTO messages(fb_threadid, fb_message_id, slack_message_timestamp, fb_message_timestamp, is_cur_read) VALUES(?,?,?,?,0)",[msg_obj.fb_threadid,msg_obj.fb_message_id,msg_obj.slack_message_timestamp,msg_obj.fb_message_timestamp], function(err, result){
            client.release();
            if(err)
                return callback(new Error("Couldn't insert into the messages table: " + err.message)); 
            callback(null);
        });
    });
}
function message_exists(fb_message_id,callback) {
    pool.getConnection(function(err, client) {
        if(err)
            return callback(new Error("Couldn't connect to mysql db: " + err.message));

        client.query("SELECT fb_message_id FROM messages WHERE fb_message_id = ?",[fb_message_id], function(err, result){
            client.release();
            if(err)
                return callback(new Error("Couldn't select from the messages table: " + err.message)); 
            callback(null,result.length != 0);
        });
    });
}
function get_cur_read_msg_on_thread(fb_threadid,callback) {
    pool.getConnection(function(err, client) {
        if(err)
            return callback(new Error("Couldn't connect to mysql db: " + err.message));

        client.query("SELECT slack_message_timestamp FROM messages WHERE is_cur_read=1 && fb_threadid = ?",[fb_threadid], function(err, result){
            client.release();
            if(err)
                return callback(new Error("Couldn't select from the messages table: " + err.message)); 
            callback(null,result.length != 0 ? result[0].slack_message_timestamp : null);
        });
    });
}

//This will get the message that is on the thread that is less or equal to the fb timestamp the user read through and set that message as is_cur_read value and clear out the old one
function get_and_set_read_msg_on_thread(fb_threadid,fb_as_new_as_timestamp,callback){
    pool.getConnection(function(err, client) {
        if(err)
            return callback(new Error("Couldn't connect to mysql db: " + err.message));
        client.query("UPDATE messages SET is_cur_read=0 WHERE is_cur_read=1 && fb_threadid=?",[fb_threadid], function(err, result){
            if(err){
                client.release();
                return callback(new Error("Couldn't select from the messages table: " + err.message)); 
            }

            client.query("SELECT fb_message_id,slack_message_timestamp FROM messages WHERE fb_message_timestamp <= ? && fb_threadid = ? ORDER BY fb_message_timestamp desc LIMIT 1",[fb_as_new_as_timestamp,fb_threadid], function(err, result){
                if(err){
                    client.release();
                    return callback(new Error("Couldn't select from the messages table: " + err.message)); 
                }
                client.query("UPDATE messages SET is_cur_read=1 WHERE fb_message_id=?",[result.length != 0 ? result[0].fb_message_id : ""], function(err, res2){
                    client.release();
                    if(err)
                        return callback(new Error("Couldn't select from the messages table: " + err.message)); 
                    callback(null,result.length != 0 ? result[0].slack_message_timestamp : null);
                });
            });
        });
    });

}

// Load the settings and JSON from mysql
function load_data(callback){
    
    pool.getConnection(function(err, client) {
        if(err){
            return callback(new Error("Couldn't connect to mysql db: " + err.message));
        }
        
        client.query("SELECT settings_json FROM settings WHERE id = 1", function(err, result){
            if(err || result.length == 0){
                client.release();
                return callback(new Error("No settings in mysql table"));
            }
            
            try {
                client.release();
                return callback(null, JSON.parse(result[0].settings_json));
            } catch (err){
                return callback("Found results in mysql table, but failed to parse: " + err);
            }
        });
    });
}

function createTableIfNeeded(client, table, create_sql, callback){
    client.query("SELECT * FROM " + table + " LIMIT 1", function(err, result){
        if(err) {
            return client.query(create_sql, callback);
        } else {
            // table exists
            return callback(null);
        }
    });
}

function save_data(data, callback){
    pool.getConnection(function(err, client) {
        if(err){
            return callback(new Error("Couldn't connect to mysql db: " + err.message));
        }
        
            var insertQuery = "INSERT INTO settings(id, settings_json) VALUES (1, ?) ON DUPLICATE KEY UPDATE settings_json=VALUES(settings_json)";
            insertQuery = mysql.format(insertQuery,[JSON.stringify(data)]);
            client.query(insertQuery, function(err, result){
            client.release();
               if(err)
                   return callback(new Error("Couldn't insert/update settings table: " + err.message)); 
               callback();
            });
    });
}

var settings = {
    token: process.env.BOT_API_KEY.trim(),
    name: process.env.BOT_NAME,
    authorised_username: process.env.AUTHORISED_USERNAME,
    debug_messages: process.env.DEBUG_MESSAGES || false,
    facebook: {
        email: process.env.FACEBOOK_EMAIL,
        pass: process.env.FACEBOOK_PASSWORD
    }
}
create_pool(function(err) {
    if (err)
        throw new Error("Error initializing pool of: " + err);
    var meta_funcs = {};
    if (process.env.METADATA_TRACKING)
        meta_funcs = {get_cur_read_msg_on_thread:get_cur_read_msg_on_thread,get_and_set_read_msg_on_thread:get_and_set_read_msg_on_thread,message_exists:message_exists,insert_message:insert_message};
    var facebot = new Facebot(settings, load_data, save_data, meta_funcs);
    facebot.run();
});
