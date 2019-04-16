/* sql_loader.cpp:
 *
 * This program compiles to create a high-performance loader of text
 * files into SQLite3 using a schema defined by schema.py.
 *
 * Follow these commands to compile it (in the directory pss/
 *
 * python3 ../txt_parser.py $(TXT_FILE) .                  (creates schema.h)
 * g++ -c -o sql_loader.o ../sql_loader.cpp $(CPPFLAGS)
 * g++ -o sql_loader -lsqlite3 sql_loader.o $(LDFLAGS)
 *
 * This is a neat demonstration of what can be done with the ETL
 * technology in this project. However, it's not clear that a compile
 * C++ loader is dramatically faster than a pure-Python
 * implementation. Certainly the string manipulations are faster,
 * though.
 */



#include <iostream>
#include <fstream>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sqlite3.h> 
#include <string.h>
#include <assert.h>
#include <getopt.h>
#include <time.h>

#include "schema.h"

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
    for(int i=0; i<argc; i++){
        printf("callback: %s = %s\n",
               azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

/* Return the integer value of line[start..end], where line[1..1] is the 0th character. */
int get_int(const std::string &line, const int start, const int end)
{
    if (start > end || line.size() <= end){
	fprintf(stderr,"get_int(%s,len=%dstart=%d,end=%d)\n",line.c_str(),line.size(),start,end);
	exit(1);
    }
    return atoi( line.substr(start-1, end-start+1).c_str());    
}

/* Copy the text into buf from line[start..end], where line[1..1] is the 0th character. */
const char *get_text(const std::string &line,
                     char *buf, const int buflen, const int start, const int end)
{
    assert(start <= end);
    assert(line.size() >= end);
    assert(buflen >= end);
    for (int i=0;i < (end-start) + 1 && i<buflen-1; i++){
        buf[i] = line[start + i - 1];
    }
    buf[end-start+1] = 0;
    return buf;
}

void error(int n)
{
    fprintf(stderr,"SQL Binding Error At Statement %d\n",n);
    exit(1);
}

void usage(const char *progname)
{
    fprintf(stderr,"usage: %s [-c] <dbfile> <inputfile>\n",progname);
    exit(1);
}

int main(int argc, char* argv[])
{
    const char *progname = argv[0];
    sqlite3 *db=0;
    char *zErrMsg = 0;
    int create = 0;
    int opt = 0;
    int t0 = time(0);

    while ((opt = getopt(argc, argv, "c")) != -1){
	switch (opt){
	case 'c':
	    create = 1;
	    break;
	default:
	    usage(progname);
	}
    }
    argc -= optind;
    argv += optind;

    printf("optind=%d argc=%d\n",optind,argc);
    if (argc<1) usage(progname);

    /* Open database */
    const char *fname = argv[0];
    printf("opening database %s\n",fname);
    int rc = sqlite3_open(fname, &db);
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(0);
    } else {
        fprintf(stdout, "Opened database successfully\n");
    }

    /* Execute SQL CREATE statement */
    if (create) {
	rc = sqlite3_exec(db, SQL_CREATE, callback, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
	    fprintf(stderr, "SQL error: %s\n", zErrMsg);
	    sqlite3_free(zErrMsg);
	    sqlite3_close(db);
	    return 0;
	}
	fprintf(stdout, "Table created successfully\n");
	exit(0);
    }

    if (argc!=2) usage(progname);

    sqlite3_exec(db, "PRAGMA cache_size = -512000", NULL, NULL, &zErrMsg); // take a 512MB
    sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &zErrMsg);
    sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", NULL, NULL, &zErrMsg);
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &zErrMsg);

    sqlite3_stmt *s=0;
    if(sqlite3_prepare_v2(db, SQL_INSERT, strlen(SQL_INSERT), &s, NULL)!=SQLITE_OK ){
        fprintf(stderr, "SQL prepare error");
        return(0);
    }
    std::ifstream f(argv[1]);
    int linenumber = 0;
    std::string line;
    if (f.is_open()){
        while(getline(f,line)){
            char buf[DATA_LINE_LEN+1];
            linenumber += 1;
            if (line.size() != DATA_LINE_LEN){
                fprintf(stderr,"line number: %d\nline len: %zu expected line len: %d\n",
                        linenumber,line.size(),DATA_LINE_LEN+1);
	    } else{
		SQL_PREPARE(s,line);
                
		rc = sqlite3_step(s);   // evaluate the SQL statement
		if(rc==SQLITE_ERROR){
		    fprintf(stderr,"sqlite3_step error???");
		    exit(1);
		}
		sqlite3_reset(s);       // resets the prepared statement
            }
        }
        f.close();
    }
    if(sqlite3_finalize(s)!=SQLITE_OK){ // delete the prepared statement
        fprintf(stderr,"sqlite3_finalize failed\n");
        exit(1);
    }
    sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &zErrMsg);
    sqlite3_free(db);                   // release the handle
    int t1 = time(0);
    int td = t1 - t0;
    if (td<1) td=1;
    printf("%s lines added: %d  seconds: %d  lines/sec: %d\n",fname,linenumber,td,linenumber/td);
    exit(0);
}
