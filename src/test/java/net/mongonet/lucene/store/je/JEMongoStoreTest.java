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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import com.sleepycat.je.DatabaseException;


import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Tests {@link JEMongoDirectory}.
 * 
 * Adapted from Andi Vajda's org.apache.lucene.db.DbStoreTest.
 *
 */
public class JEMongoStoreTest extends LuceneTestCase {
    protected File dbHome = new File(TEMP_DIR,"index");

    protected Environment env;
    protected DatabaseConfig dbConfig;
    protected EnvironmentConfig envConfig;
    //    protected Database index, blocks;

    @Override
    public void setUp() throws Exception {
      super.setUp();

        if (!dbHome.exists())
            dbHome.mkdir();
        else {
            File[] files = dbHome.listFiles();

            for (int i = 0; i < files.length; i++) {
                String name = files[i].getName();
                if (name.endsWith("jdb") || name.equals("je.lck"))
                    files[i].delete();
            }
        }

	envConfig= new EnvironmentConfig();
        dbConfig = new DatabaseConfig();

        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        env = new Environment(dbHome, envConfig);

    }

    @Override
    public void tearDown() throws Exception {

	/*        if (index != null)
            index.close();
        if (blocks != null)
            blocks.close();
        if (env != null)
            env.close();
	*/
        if (env != null)
            env.close();

        super.tearDown();
    }

    public void testBytes() throws Exception {
        final int count = 250;
        final int LENGTH_MASK = 0xffff;

        Random r = random;
        final long seed = r.nextLong();
        Random gen = new Random(seed);
        int totalLength = 0;
        int duration;
        Date end;

        Date veryStart = new Date();
        Date start = new Date();

        JEMongoDirectory store = null;

        if (VERBOSE) System.out.println("Writing files byte by byte");

        try {
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexOutput file = store.createOutput(name);

                totalLength += length;

                for (int j = 0; j < length; j++) {
                    byte b = (byte) (gen.nextInt() & 0x7F);
                    file.writeByte(b);
                }

                file.close();
            }
	    store.commitTransaction();

        } catch (IOException e) {
	    store.abortTransaction();
        } finally{
	    store.close();
	}

        end = new Date();

        if (VERBOSE) {
          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to create, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {
         
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexInput file = store.openInput(name);

                if (file.length() != length)
                    throw new Exception("length incorrect");

                for (int j = 0; j < length; j++) {
                    byte b = (byte) (gen.nextInt() & 0x7F);

                    if (file.readByte() != b)
                        throw new Exception("contents incorrect");
                }

                file.close();
            }
        } catch (IOException e) {
            store.abortTransaction();
	    throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
	    throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to read, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                store.deleteFile(name);
            }
        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();

            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          System.out.print(end.getTime() - start.getTime());
          System.out.println(" total milliseconds to delete");

          System.out.print(end.getTime() - veryStart.getTime());
          System.out.println(" total milliseconds");
        }
    }

    public void testDelete() throws Exception {
        final int count = 250;
        final int LENGTH_MASK = 0xffff;

        Random r = random;
        final long seed = r.nextLong();
        Random gen = new Random(seed);
        int totalLength = 0;
        int duration;
        Date end;

        Date veryStart = new Date();
        Date start = new Date();

        JEMongoDirectory store = null;

        if (VERBOSE) System.out.println("Writing files byte by byte");

        try {
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();
            for (int i = 0; i < count; i++) {
		String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexOutput file = store.createOutput(name);

                totalLength += length;

                for (int j = 0; j < length; j++) {
                    byte b = (byte) (gen.nextInt() & 0x7F);
                    file.writeByte(b);
                }

                file.close();
            }
        } catch (IOException e) {
            store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to read, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {

            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();

            for (int i = 0; i < count; i++) {
                if (i % 2 == 0) {
                    String name = i + ".dat";
                    store.deleteFile(name);
                }
            }
        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          System.out.print(end.getTime() - start.getTime());
          System.out.println(" total milliseconds to delete even files");

          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to create, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();

            for (int i = 0; i < count; i++) {
                int length = gen.nextInt() & LENGTH_MASK;

                if (i % 2 != 0) {
                    String name = i + ".dat";
                    IndexInput file = store.openInput(name);
                    if (file.length() != length)
                        throw new Exception("length incorrect");

                    for (int j = 0; j < length; j++) {
                        byte b = (byte) (gen.nextInt() & 0x7F);

                        if (file.readByte() != b)
                            throw new Exception("contents incorrect");
                    }

                    file.close();
                } else {
                    for (int j = 0; j < length; j++) {
                        gen.nextInt();
                    }
                }
            }
        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to read, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();

            for (int i = 0; i < count; i++) {
                if (i % 2 != 0) {
                    String name = i + ".dat";
                    store.deleteFile(name);
                }
            }

        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          System.out.print(end.getTime() - start.getTime());
          System.out.println(" total milliseconds to delete");

          System.out.print(end.getTime() - veryStart.getTime());
          System.out.println(" total milliseconds");
        }

        Cursor cursor = null;
        try {
            store = new JEMongoDirectory(env,dbConfig);

	    try {
		cursor = store.getIndex().openCursor(null, null);
		
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		if (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		    fail("index database is not empty");
		}
	    } catch (DatabaseException e) {
		throw e;
	    } finally {
		if (cursor != null)
		    cursor.close();
	    }

	    cursor = null;
	    try {
		cursor = store.getBlocks().openCursor(null, null);

		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		if (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		    fail("blocks database is not empty");
		}
	    } catch (DatabaseException e) {
		throw e;
	    } finally {
		if (cursor != null)
		    cursor.close();
	    }
	} catch (DatabaseException e) {
	    throw e;
	} finally {
	    store.close();
	}
    }

    public void testArrays() throws Exception {
        final int count = 250;
        final int LENGTH_MASK = 0xffff;

        Random r = random;
        final long seed = r.nextLong();
        Random gen = new Random(seed);
        int totalLength = 0;
        int duration;
        Date end;

        Date veryStart = new Date();
        Date start = new Date();

        JEMongoDirectory store = null;

        if (VERBOSE) System.out.println("Writing files as one byte array");

        try {
            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexOutput file = store.createOutput(name);
                byte[] data = new byte[length];

                totalLength += length;
                gen.nextBytes(data);
                file.writeBytes(data, length);

                file.close();
            }
        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();


            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to create, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {

            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexInput file = store.openInput(name);

                if (file.length() != length)
                    throw new Exception("length incorrect");

                byte[] data = new byte[length];
                byte[] read = new byte[length];
                gen.nextBytes(data);
                file.readBytes(read, 0, length);

                if (!Arrays.equals(data, read))
                    throw new Exception("contents incorrect");

                file.close();
            }
        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          duration = (int) (end.getTime() - start.getTime());
          System.out.print(duration);
          System.out.print(" total milliseconds to read, ");
          System.out.print(totalLength / duration);
          System.out.println(" kb/s");
        }

        try {

            store = new JEMongoDirectory(env,dbConfig);
	    store.beginTransaction();

            gen = new Random(seed);
            start = new Date();
            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                store.deleteFile(name);
            }

        } catch (IOException e) {
	    store.abortTransaction();
            throw e;
        } catch (DatabaseException e) {
	    store.abortTransaction();
            throw e;
        } finally {
	    store.commitTransaction();
            store.close();
        }

        end = new Date();

        if (VERBOSE) {
          System.out.print(end.getTime() - start.getTime());
          System.out.println(" total milliseconds to delete");

          System.out.print(end.getTime() - veryStart.getTime());
          System.out.println(" total milliseconds");
        }
    }
}
