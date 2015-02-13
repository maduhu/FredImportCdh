/**
 *    Copyright 2015 IPC Global (http://www.ipc-global.com) and others.
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.ipcglobal.fredimportcdh;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ipcglobal.fredimport.util.FredUtils;
import com.ipcglobal.fredimport.util.LogTool;


/**
 * The Class TsvsToImpala.
 */
public class TsvsToImpala {
	
	/** The Constant log. */
	private static final Log log = LogFactory.getLog(TsvsToImpala.class);
	
	/** The properties. */
	private Properties properties;
	
	/** The hdfs staging directory. */
	private String hdfsStagingDirectory;
	
	/** The hdfs staging directory tsvs. */
	private String hdfsStagingDirectoryTsvs;
	
	/** The path table tsv files. */
	private String pathTableTsvFiles;
	
	/** The conf. */
	private Configuration conf;
	
	/** The file system. */
	private FileSystem fileSystem;

	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		LogTool.initConsole();
		if( args.length < 1 ) {
			log.error("Properties path/name is required");
			System.exit(8);
		}
		
		TsvsToImpala tsvsToImpala = null;
		try {
			tsvsToImpala = new TsvsToImpala( args[0] );
			tsvsToImpala.process();

		} catch (Exception e) {
			log.info(e);
			e.printStackTrace();
		}
	}

	/**
	 * Instantiates a new tsvs to impala.
	 *
	 * @param pathNameProperties the path name properties
	 * @throws Exception the exception
	 */
	public TsvsToImpala( String pathNameProperties ) throws Exception {
		this.conf = new Configuration();
		this.properties = loadProperties( pathNameProperties, this.conf );
		this.hdfsStagingDirectory = this.properties.getProperty("hdfsStagingDirectory").trim();
		this.hdfsStagingDirectoryTsvs = this.properties.getProperty("hdfsStagingDirectoryTsvs").trim();		
		String outputPath = FredUtils.readfixPath( "outputPath", properties );		
		String outputSubdirTableTsvFiles = FredUtils.readfixPath( "outputSubdirTableTsvFiles", properties );
		this.pathTableTsvFiles = outputPath + outputSubdirTableTsvFiles;
		this.fileSystem = FileSystem.get( this.conf );
	}

	
	/**
	 * Process.
	 *
	 * @throws Exception the exception
	 */
	public void process( ) throws Exception {
		try {
			
			log.info("Start: Delete HDFS Staging Directory");
			long before = System.currentTimeMillis();
			fileSystem.delete( new Path(hdfsStagingDirectory), true );
			log.info("Complete: Delete HDFS Staging Directory, elapsed=" + (System.currentTimeMillis()-before ));
			
			log.info("Start: Copy TSVs To HDFS Staging");
			before = System.currentTimeMillis();
			fileSystem.mkdirs( new Path(hdfsStagingDirectory) );
			fileSystem.copyFromLocalFile(new Path(pathTableTsvFiles), new Path(hdfsStagingDirectory) );
			log.info("Complete: Copy TSVs To HDFS Staging, elapsed=" + (System.currentTimeMillis()-before ));
			
			log.info("Start: Process Tables");
			before = System.currentTimeMillis();
			processTables();
			log.info("Complete: Process Tables, elapsed=" + (System.currentTimeMillis()-before ));
					
			log.info("Start: Delete HDFS Staging Directory");
			before = System.currentTimeMillis();
			fileSystem.delete( new Path(hdfsStagingDirectory), true );
			log.info("Complete: Delete HDFS Staging Directory, elapsed=" + (System.currentTimeMillis()-before ));
	
		} catch( Exception e ) {
			log.error(e);
			throw e;
		}
	}
	

	/**
	 * Process tables.
	 *
	 * @throws Exception the exception
	 */
	private void processTables() throws Exception {
		String jdbcImpalaDriverClass = this.properties.getProperty("jdbcImpalaDriverClass").trim();
		String jdbcImpalaUrl = this.properties.getProperty("jdbcImpalaUrl").trim();
		String jdbcImpalaLogin = this.properties.getProperty("jdbcImpalaLogin").trim();
		String jdbcImpalaPassword = this.properties.getProperty("jdbcImpalaPassword").trim();

		String databaseNameFred = this.properties.getProperty("databaseNameFred").trim();
		String tableNameFred = properties.getProperty("tableNameFred");
		String stageTableSuffix = "_staging";
		Connection con = null;
		Statement statement = null;
		
		String databaseTableName = databaseNameFred + "." + tableNameFred;
		String databaseStageTableName = databaseNameFred + "." + tableNameFred + stageTableSuffix;
				 
		try {

			Class.forName(jdbcImpalaDriverClass);
			con = DriverManager.getConnection( jdbcImpalaUrl, jdbcImpalaLogin, jdbcImpalaPassword);
			statement = con.createStatement();

			log.info( "\tDrop/create internal and external staging table" );
			statement.executeUpdate( "DROP TABLE IF EXISTS " + databaseTableName );
			statement.executeUpdate( "DROP TABLE IF EXISTS " + databaseStageTableName );
			
			log.info( "\tCreate external staging table against the tsv's in HDFS" );
			statement.executeUpdate( createStageTableStatement( databaseStageTableName ) );			
			statement.executeUpdate( "REFRESH " + databaseStageTableName );		
			
			log.info( "\tCreate internal table as select from external staging table" );
			String sqlCreateAs = "create table " + databaseTableName + " stored as parquet as select * from " + databaseStageTableName;
			statement.executeUpdate( sqlCreateAs );
			statement.executeUpdate( "REFRESH " + databaseTableName );

			log.info( "\tDrop the staging table" );
			statement.executeUpdate( "DROP TABLE IF EXISTS " + databaseStageTableName );

			log.info( "\tStart - COMPUTE STATS on internal table" );
			String sqlComputeStats = "COMPUTE STATS " + databaseTableName;
			try {
				long before = System.currentTimeMillis();
				log.info( sqlComputeStats );
				statement.executeUpdate( sqlComputeStats );
				log.info("\tCOMPUTE STATS Insert Ok, Elapsed=" + convertMSecsToHMmSs(System.currentTimeMillis()-before) );

			} catch( Exception e ) {
				log.error("COMPUTE STATS, statement=" + sqlComputeStats, e);
				throw e;
			}
			
		} catch( SQLException se ) {
			log.error(se);
			log.error( "Cause: " + se.getCause() );
			log.error( "getErrorCode: " + se.getErrorCode() );
			log.error( "getSQLState: " + se.getSQLState() );
			se.printStackTrace();
			throw se;
		} catch( Exception e ) {
			log.error(e);
			e.printStackTrace();
			throw e;
		} finally {
			try { if( statement != null ) statement.close(); } catch( Exception e ) {}
			try { if( con != null ) con.close(); } catch( Exception e ) {}
		}
	}


	/**
	 * Load properties.
	 *
	 * @param propFileNameExt the prop file name ext
	 * @param conf the conf
	 * @return the properties
	 * @throws Exception the exception
	 */
	public static Properties loadProperties( String propFileNameExt, Configuration conf ) throws Exception {
		InputStream input = null;
		Properties properties = new Properties();
		try {
			Path pathPropFile = new Path( propFileNameExt );
			FileSystem fsPropFile = pathPropFile.getFileSystem( conf );
			input = fsPropFile.open(pathPropFile);
			// input = new FileInputStream(propFileNameExt);
			properties.load(input);
			return properties;
		} catch (IOException ex) {
			ex.printStackTrace();
			throw ex;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	
	/**
	 * Convert m secs to h mm ss.
	 *
	 * @param msecs the msecs
	 * @return the string
	 */
	public static String convertMSecsToHMmSs(long msecs) {
		return convertSecsToHMmSs(msecs/1000L);
	}
	
	
	/**
	 * Convert secs to h mm ss.
	 *
	 * @param seconds the seconds
	 * @return the string
	 */
	public static String convertSecsToHMmSs(long seconds) {
	    long s = seconds % 60;
	    long m = (seconds / 60) % 60;
	    long h = (seconds / (60 * 60)) % 24;
	    return String.format("%02d:%02d:%02d", h,m,s);
	}

	
	/**
	 * Creates the stage table statement.
	 *
	 * @param databaseStageTableName the database stage table name
	 * @return the string
	 */
	public String createStageTableStatement( String databaseStageTableName ) {
		String createTable = 
			"CREATE EXTERNAL TABLE " + databaseStageTableName + " ( " +
				"category1 string, " +
				"category2 string, " +
				"category3 string, " +
				"category4 string, " +
				"category5 string, " +
				"category6 string, " +
				"category7 string, " +
				"category8 string, " +
				"category9 string, " +
				"category10 string, " +
				"category11 string, " +
				"category12 string, " +
				"units string, " +
				"frequency string, " +
				"seasonal_adj string, " +
				"last_updated string, " +
				"date_series string, " +
				"value decimal(38,20), " +
				"country string, " +
				"city string, " +
				"county string, " +
				"state string, " +
				"region_us string, " +
				"region_world string, " +
				"institution string, " +
				"frb_district string, " +
				"sex string, " +
				"currency string " +
			") \n" +
			"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'  \n" +
			"STORED AS TEXTFILE \n" +
			"LOCATION '" + hdfsStagingDirectoryTsvs + "'";
		return createTable;
	}

	
}
