package com.teraim.sluconvert;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.teraim.fieldapp.synchronization.SyncEntry;



public class Convert {

	final static int MAX_ROWS_TO_RETURN = 10;

	public static void main(String[] args) {



		int ID=0;
		Convert conv = new Convert();
		Connection con = conv.connectDatabase();

		try {
			if (false) {
				Statement stm = con.createStatement();
				ResultSet r = stm.executeQuery("SELECT * FROM dbo.audit WHERE ID = 1761");
				if (!r.next()) {
					System.out.println("No RESULTSET");
					return;
				}

				Blob blobr = r.getBlob("SYNCOBJECTS");
				byte[] bytez = blobr.getBytes(1,(int)blobr.length());
				if (bytez!=null) {
					//Write BLOB!!
					ByteArrayInputStream in = new ByteArrayInputStream(bytez);
					ObjectInputStream is = new ObjectInputStream(in);
					SyncEntry[] sexx = (SyncEntry[])is.readObject();
					for (SyncEntry s:sexx) {
						if (s==null || s.getKeys()==null || s.getValues()==null) {
							System.out.println("NULL!!");
						} //else
						//System.out.println("key: "+s.getKeys().toString()+"values: "+s.getValues().toString());
						//System.out.println(s.getChange());
						try {
							boolean f = s.getChange().length()>2000 |s.getAuthor().length()>100 | s.getTarget().length()>255;
							if (f) {
								System.out.println("ALARM");
								System.out.println("CH: "+s.getChange());
							}
						} catch (Exception e) {
							System.out.println("exc: "+e.getMessage());
						}

					}
					in.close();
					is.close();
					if (args.length==0)
						return;
				} else {
					System.out.println("NULL bytes");
					return;
				}
			}

			

			PreparedStatement stmt = con.prepareStatement("select TOP "+MAX_ROWS_TO_RETURN +" * FROM audit WHERE TIMEOFINSERT > ? AND ID > ?");
			PreparedStatement in_conv_stmt = con.prepareStatement("INSERT INTO [dbo].[convert] "
					+ "( [SYNCGROUP]"
					+ ",[USER]"
					+ ",[APP]"
					+ ",[TIMEOFINSERT]"
					+ ",[TYPE]"
					+ ",[CHANGES]"
					+ ",[TIMEINENTRY]"
					+ ",[TARGET]"
					+ ",[AUTHOR]"
					+ ",[ORIG_ID]"
					+ ",[DUP] )"
					+ " VALUES (?,?,?,?,?,?,?,?,?,?,?)");
			PreparedStatement in_loc_stmt = con.prepareStatement("INSERT INTO [dbo].[location] "
					+ "( [SYNCGROUP]"
					+ ",[USER]"
					+ ",[APP]"
					+ ",[TIMEOFINSERT]"
					+ ",[TYPE]"
					+ ",[TIMEINENTRY]"
					+ ",[AUTHOR]"
					+ ",[ORIG_ID]"
					+ ",[DUP]"
					+ ",[X] "
					+ ",[Y] "
					+ ",[ACCURACY] "
					+ " )"
					+ " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

			PreparedStatement in_stmt;


			String user,target;
			Timestamp timeOfInsert;
			final class GeoLocation {

				float X=-1f;
				float Y=-1f;
				float acc=-1;
				
				public void insertX(float X) {
					if (this.X!=-1) {
						p("X already had value...reset");
						reset();
					}
					this.X=X;
				}
				public void insertY(float Y) {
					if (this.Y!=-1) {
						p("Y already had value...reset");
						reset();
					}
					this.Y=Y;
				}

				public void setAccuracy(float acc) {
					this.acc=acc;
				}

				public boolean isReady() {
					return (Y!=-1 && X!=-1);
				}

				private void reset() {
					X=-1f;
					Y=-1f;
				}

			}

			//Create a map that holds a geolocation per user. 
			Map<String,GeoLocation> userLocations = new HashMap();
			GeoLocation curr_geo=null; 

			//Create map that holds timeentries. If timeentry has been seen, skip.
			Map <Long,Integer> currentStamps = new HashMap();
			Timestamp maxTime=null;
			
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = dateFormat.parse("2017-10-16 09:38:37.477");
			long time = date.getTime(),totalCountL=0,totalCountD=0,totalCountDM=0,totalCountI=0,skipC=0,alarmC=0,nullB=0;
			int lastSeenIndex = 0;
			Timestamp timeForLastEntry = new Timestamp(time);
			int DUP = 0;
			while (true) {

				stmt.setTimestamp(1, timeForLastEntry);
				stmt.setInt(2, lastSeenIndex);
				ResultSet resultSet = stmt.executeQuery();
				//ResultSetMetaData rsmd = resultSet.getMetaData();
				//int columnsNumber = rsmd.getColumnCount();
				boolean initial = true;

				long TIMEINENTRY; 
				while (resultSet.next()) {
					initial = false;
					int id = resultSet.getInt("id");
					if (id > lastSeenIndex)
						lastSeenIndex = id;
					Blob blob = resultSet.getBlob("SYNCOBJECTS");
					byte[] bytes = blob.getBytes(1,(int)blob.length());
					if (bytes!=null) {
						//Write BLOB!!
						ByteArrayInputStream in = new ByteArrayInputStream(bytes);
						ObjectInputStream is = new ObjectInputStream(in);
						SyncEntry[] sex = (SyncEntry[])is.readObject();
						if(sex!=null) {
							String _prevU = null;
							for(SyncEntry se:sex) {
								TIMEINENTRY = se.getTimeStamp();
								String _user = se.getAuthor();
								if (_prevU != null) {
									if (!_user.equals(_prevU))
										alarmC++;

								} else
									_prevU = _user;
								ID = resultSet.getInt("ID");
								DUP = 0;
								if (currentStamps.get(TIMEINENTRY)!=null) { 
									System.out.println(se.getAuthor()+":"+se.getTimeStamp()+"my ID: "+ID+" previous ID: "+currentStamps.get(TIMEINENTRY));
									skipC++;
									DUP = 1;
									//continue;
								} else
									currentStamps.put(TIMEINENTRY,ID);
								//check for null
								//if se.getChange()
								int p = 1;
								
								user = resultSet.getString("USER");
								target = se.getTarget();
								timeOfInsert = resultSet.getTimestamp("TIMEOFINSERT");


								if (se.isInsertArray()) {
									in_stmt = in_loc_stmt;
									//get the current geoLocation for this user. 
									curr_geo = userLocations.get(user);
									if (curr_geo == null) {
										curr_geo = new GeoLocation();
										userLocations.put(user,curr_geo);
									}
									float value = Float.parseFloat((String)se.getValues().get("value"));
									//p("xiidd "+ID+"t: "+target);
									if ("GPS_X".equals(target)) 
										curr_geo.insertX(value);

									else if ("GPS_Y".equals(target)) 
										curr_geo.insertY(value);
									else if ("GPS_Accuracy".equals(target)) 
										curr_geo.setAccuracy(value);

									if (!curr_geo.isReady()) {
										//only insert if ready
										continue;
									} 
								}
								else 
									in_stmt = in_conv_stmt;

								in_stmt.setString(p++, resultSet.getString("SYNCGROUP"));
								in_stmt.setString(p++, user);
								in_stmt.setString(p++, resultSet.getString("APP"));

								in_stmt.setTimestamp(p++, timeOfInsert);
								in_stmt.setString(p++, se.isInsert()?"I":se.isDelete()?"D":se.isDeleteMany()?"M":se.isInsertArray()?"A":"?");
								if (!se.isInsertArray()) {
									String ch = se.getChange();
									if (ch!=null && ch.length()>2000)
										ch = ch.substring(0, 1999);
									in_stmt.setString(p++, ch);
								}
								in_stmt.setLong(p++, TIMEINENTRY);
								if (!se.isInsertArray())
									in_stmt.setString(p++, target);
								in_stmt.setString(p++, se.getAuthor());
								in_stmt.setInt(p++,ID); 							
								in_stmt.setInt(p++,DUP);
								if (se.isInsertArray()) {
									totalCountL++;
									in_stmt.setFloat(p++,curr_geo.X);
									in_stmt.setFloat(p++,curr_geo.Y);
									in_stmt.setFloat(p++,curr_geo.acc);
									curr_geo.reset();
								}
								else if (se.isDelete())
									totalCountD++;
								else if (se.isDeleteMany())
									totalCountDM++;
								else if (se.isInsert())
									totalCountI++;
								in_stmt.executeUpdate();
								//check if this timestamp is bigger than max
								if (maxTime==null || timeOfInsert.after(maxTime))
									maxTime = timeOfInsert;
								//else
								//	p("timeofinsert "+timeOfInsert+"is not after "+maxTime);

							}
							sex = null;
						} 
						is.close();in.close();
					} else 
						nullB++;
				}
				if (initial) {
					System.out.println("Done.");
					return;
				}

				else {
					timeForLastEntry = maxTime;
					
				}
				System.out.println("I: "+lastSeenIndex+" L: "+totalCountL+" D: "+totalCountD+" DM: "+totalCountDM+" I: "+totalCountI+" Time: "+timeForLastEntry+" Dup: "+skipC+" A: "+alarmC+" B: "+nullB);			
			}

		} catch (Exception e) {

			e.printStackTrace();
			System.out.println("ID for line was "+ID);
		}
	}

	private Connection connectDatabase() {
		final String connectionUrl = "jdbc:sqlserver://ebdb.cljwr0n66av2.eu-west-1.rds.amazonaws.com;user=kalle;password=AbraKadabra!1;databaseName=Rlo_prod;";
		Connection con=null;
		try {

			con = DriverManager.getConnection(connectionUrl);
			if (con==null)
				System.err.println("Connection to database failed.");


		} catch (SQLException e) {

			e.printStackTrace();

		}
		return con;
	}

	static void p(String s) {
		System.out.println(s);
	}
}
