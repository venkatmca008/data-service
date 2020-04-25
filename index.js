const fs = require('fs');
const mysql = require('mysql');
const csv = require('fast-csv');

var RDS_HOSTNAME = process.env.RDS_HOSTNAME 
var RDS_USERNAME = process.env.RDS_USERNAME 
var RDS_PASSWORD = process.env.RDS_PASSWORD 


let stream = fs.createReadStream("airlines.dat");
let myData = [];
let csvStream = csv
    .parse()
    .on("data", function (data) {
        myData.push(data);
    })
    .on("end", function () {
        myData.shift();

        // create a new connection to the database
        const connection = mysql.createConnection({
            host: RDS_HOSTNAME,
            user: RDS_USERNAME,
            password: RDS_PASSWORD,
            database: 'quantum',
            ssl: true
        });

        // open the connection
        connection.connect((error) => {
            if (error) {
                console.error(error);
            } else {


                let checktable = `SELECT count(*) as count FROM information_schema.tables
                WHERE table_schema = 'quantum'
                AND table_name = 'quantumairline'`


                connection.query(checktable, function (err, results) {
                    if (err) {
                        console.log(err.message);
                    }

                    if (results[0].count <= 0) {

                        let createquantum = `CREATE TABLE quantum.quantumairline (
                            sno INT NOT NULL,
                            flightname VARCHAR(100) NULL,
                            col VARCHAR(45) NULL,
                            col2 VARCHAR(95) NULL,
                            col3 VARCHAR(95) NULL,
                            col4 VARCHAR(95) NULL,
                            col5 VARCHAR(95) NULL,
                            col6 VARCHAR(95) NULL,
                            PRIMARY KEY (sno));`;
                        connection.query(createquantum, function (err, results, fields) {
                            if (err) {
                                console.log(err.message);
                            }
                        });
                    }

                });

                let query = 'INSERT INTO quantumairline (sno, flightname,col,col2,col3,col4,col5,col6) VALUES ?';
                connection.query(query, [myData], (error, response) => {
                    console.log(error || response);
                });

                connection.end();
            }
        });
    });

stream.pipe(csvStream);
