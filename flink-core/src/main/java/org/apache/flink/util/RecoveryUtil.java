/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.io.File;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;

public class RecoveryUtil {
	
	private static String newPath;
	
	public static String getCheckpointPath() {
		if(newPath != null) {
			return newPath;
		}
		
		String default_tmp_path = ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH.split(File.pathSeparator)[0];
		
		char lastchar = default_tmp_path.charAt(default_tmp_path.length()-1);
		
		if(lastchar != File.separatorChar)
		{
			default_tmp_path = default_tmp_path + File.separator;
		}
		
		return GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_CHECKPT_DIR_KEY, 
				GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
						default_tmp_path));
	}
	
	public static String getLoggingPath() {
		return ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH;
	}
	
	public static void setNewPath(String path) {
		newPath = path;
	}
}
