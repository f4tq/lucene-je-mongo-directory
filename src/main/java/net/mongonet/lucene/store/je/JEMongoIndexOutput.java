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

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import net.mongonet.lucene.store.je.JEMongoDirectory;
import org.apache.lucene.store.je.JEIndexOutput;
/**
 * Port of Andi Vajda's DbDirectory to Java Edition of Berkeley Database
 *
 */

public class JEMongoIndexOutput  extends JEIndexOutput {


    protected JEMongoIndexOutput(JEMongoDirectory directory, String name, boolean create)
            throws IOException {
        super(directory,name,create);
    }

    @Override
    public void close() throws IOException {
	super.close();
	
	((JEMongoDirectory)directory).commitTransaction();
    }

}
