package net.mongonet.lucene.store.je;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import com.sleepycat.je.Environment;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.TransactionConfig;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import java.io.IOException;
import org.apache.lucene.store.je.JEDirectory;
import org.apache.lucene.store.je.File;
/**
 *
 * Port of Port of Andi Vajda's DbDirectory to to Java Edition of Berkeley Database
 * 
 * A JEDirectory is a Berkeley DB JE based implementation of
 * {@link org.apache.lucene.store.Directory Directory}. It uses two
 * {@link com.sleepycat.je.Database Db} database handles, one for storing file
 * records and another for storing file data blocks.
 *
 */

public class JEMongoDirectory extends JEDirectory {

    protected Environment env;
    protected DatabaseConfig dbConfig;




    /**
     * Instantiate a DbDirectory. The same threading rules that apply to
     * Berkeley DB handles apply to instances of DbDirectory.
     * 
     * @param env
     *            an Environment handle that can be used to extract database handles for 
     *            index,blocks as well as manage transactionstransaction handle that is going to be used for all db
     *            operations done by this instance. This parameter may be NOT <code>null</code>.
     *            NOTE: the caller controls the Environment and is responsible for env.close() ing it!
     *
     * @param dbConfig 
     *            a Database config used to open databases with the given environment
     */

    public JEMongoDirectory(Environment env, DatabaseConfig dbConfig) throws DatabaseException, IOException
    {
	super(null,null,null,0);
	this.env = env;
	this.dbConfig = dbConfig;
	try {
	    beginTransaction();
	    this.files = env.openDatabase(txn, "__index__", dbConfig);
	    this.blocks = env.openDatabase(txn, "__blocks__", dbConfig);
	} 
	catch (DatabaseException e) {
	    abortTransaction();
	    this.files = null;
	    this.blocks = null;
	    throw e;
	} 
	finally {
	    commitTransaction();
	}
	
    }

    @Override
    public void close() throws IOException {
        flush();
	if (txn != null)
	    {
		System.err.println("WARNING: JEDirectory.close -- auto commiting open transaction!");
		abortTransaction();
	    }
	if (files != null)
            files.close();
        if (blocks != null)
            blocks.close();
    }


    /*
     *  Creates a file from the directory.  If no transaction is active, one is started with
     *  the given TransactionConfig.  To revert this method to old behaviour simply use setTransaction 
     *
     *  @name
     *       Name of file to delete
     *
     */

    @Override
    public IndexOutput createOutput(String name) throws IOException {
	if (!transactionInProgress())
	    beginTransaction();

        return new JEMongoIndexOutput(this, name, true);
    }

    /*
     *  Deletes a file from the directory.  If no transaction is active, one is started with
     *  the given TransactionConfig.  To revert this method to old behaviour simply use setTransaction 
     *
     *  @name
     *       Name of file to delete
     *
     */
    public class JEMongoFile extends org.apache.lucene.store.je.File
    {
	public JEMongoFile(String name) throws IOException{
	    super(name);
	}
	protected void delete(JEMongoDirectory directory) throws IOException {
	    super.delete(directory);
	}
    }
    @Override
     public void deleteFile(String name) throws IOException {
	if (!transactionInProgress()){
	    try {
		beginTransaction();
		new JEMongoFile (name).delete(this);
		commitTransaction();
	    }
	    catch(Exception e)
		{
		    abortTransaction();
		}
	}
	else
	    new JEMongoFile(name).delete(this);
    }


    public void beginTransaction() throws IOException{
	this.beginTransaction(null,null);
    }
    
    public void beginTransaction(Transaction parent, TransactionConfig transConfig) throws IOException{
	if (txn != null){
	    System.err.println("WARNING: JEDirectory.beginTransaction -- transaction already open.  Aborting it!");
	    abortTransaction();
	}
	try {
	    txn = env.beginTransaction(parent, transConfig);
	}
	catch(DatabaseException e){
	    throw new IOException(e);
	}
    }
    public void commitTransaction() throws IOException{
	if (txn != null)
	    {
		try {
		    txn.commit();
		}
		catch(DatabaseException e){
		    txn = null;
		    throw new IOException(e);
		}
		txn = null;
	    }
    }
    public void abortTransaction() throws IOException{
	if (txn != null)
	    try {
		txn.abort();
	    }
	    catch(DatabaseException e){
		txn = null;
		throw new IOException(e);
	    }
	txn = null;
    }
    public boolean transactionInProgress(){
	return txn != null;
    }
    public boolean good(){
	return this.env != null && this.files != null && this.blocks != null;
    }
    public Database getIndex(){
	return files;
    }
    public Database getBlocks(){
	return blocks;
    }
    public Environment getEnvironment()
    {
	return env;
    }
    public DatabaseConfig getDatabaseConfig(){
	return this.dbConfig;
    }
}
