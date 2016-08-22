package ljm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class mysqltohabse {

	private static Logger log = Logger.getLogger(mysqltohabse.class);

	private static Configuration setHbaseConf() {
		Configuration conf=new Configuration();
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "zjmaster:2181,zjslave1:2181,zjslave2:2181,zjslave3:2181");
		return conf;
	}
	public static Boolean creatHTable(String HTableName,String[] columnFamilys)  {
		
	try {
		HBaseAdmin admin=new HBaseAdmin(setHbaseConf());
		if (admin.tableExists(HTableName)) {
			admin.close();
			return false;
		} else {
			HTableDescriptor tableDescriptor=new HTableDescriptor(HTableName);
			for(String colFamily:columnFamilys)
			{
				tableDescriptor.addFamily( new HColumnDescriptor(colFamily));
			}
			admin.createTable(tableDescriptor);
			admin.close();
			return true;
		}
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return false;
	}
		
	}
	public static String loadJSON (String lat,String lng) {
		StringBuilder sb_lat=new StringBuilder(lat);
		StringBuilder sb_lng=new StringBuilder(lng);
		sb_lat.insert(2, '.');
		sb_lng.insert(3, '.');
		String url="http://api.map.baidu.com/geocoder/v2/?ak=E4805d16520de693a3fe707cdc962045&callback=renderReverse&location="+
	sb_lat.toString()+","+sb_lng.toString()+"&output=json&pois=1";
	       StringBuilder json = new StringBuilder();
	       try {
	           URL url2 = new URL(url);
	           URLConnection urlConnection = url2.openConnection();
	           BufferedReader in = new BufferedReader(new InputStreamReader(
	        		   urlConnection .getInputStream(),"UTF-8"));
	           String inputLine = null;
	           while ( (inputLine = in.readLine()) != null) {
	               json.append(inputLine);
	           }
	           in.close();
	       } catch (MalformedURLException e) {
	       } catch (IOException e) {
	       }
	       return json.toString();

	   }
	public static String getRowkey(String id,String deviceId)
	{
		String[]a_id=id.split("_");
		a_id[0]=a_id[0].substring(6,11);
		deviceId=deviceId.substring(3,10);
		return deviceId+a_id[0]+a_id[1];
	}
	public static void getDataFormMysqlToHbase(String tableName) throws IOException {
		BasicConfigurator.configure();
		log.setLevel(Level.DEBUG);
		ResultSet resultSet=null;
		DBHelper db=new DBHelper();
		String sql="SELECT  UniqueID,DeviceID,`Event`,Longitude,Latitude FROM "+
				tableName+" WHERE  `Event`=0 OR `Event`=1 ORDER  BY DeviceID";
		String base="base";
		String pois="pois";
		String[]qbase={"deviceID","event","longitude","latitude","formatted_address","business","province","city","district","street","street_number"};
		String[]qpois={"addr","name","poiType","point_x","point_y","tag"};
		String rowkey=null;
		JSONObject jsonObject=null;
		JSONObject jsonResult=null;
		JSONObject jsonLacation=null;
		JSONObject jsonAddressComponent=null;
		JSONArray jsonPois=null;
        JSONObject jsonPoi=null;
        JSONObject poiLacation=null;
        Iterator<Object> itPois=null;
        List<Put> stlList = new ArrayList<Put>();
        List<Put> poiList = new ArrayList<Put>();
        Get get=null;
        Result poiResult=null;
		try {
			resultSet=db.pst.executeQuery(sql);
			log.debug(tableName + ":start copying data to hbase...");
			HTable stlTable = new HTable(setHbaseConf(), tableName);
			HTable poiTable = new HTable(setHbaseConf(), "pois");
			while (resultSet.next()) {
				jsonObject=JSONObject.fromObject(loadJSON(resultSet.getString("Latitude"), resultSet.getString("Longtitude")));
				if(jsonObject.get("status").toString().equals('0')){
		
				jsonResult=jsonObject.getJSONObject("result");
				jsonLacation=jsonResult.getJSONObject("location");
				jsonAddressComponent=jsonResult.getJSONObject("addressComponent");
				jsonPois=jsonResult.getJSONArray("pois");
				itPois=jsonPois.iterator();
				rowkey=getRowkey(resultSet.getNString("UniqueID"), resultSet.getString("DeviceID"));
				Put stlPut=new Put(Bytes.toBytes(rowkey));
				stlPut.add(base.getBytes(), qbase[0].getBytes(), resultSet.getString("DeviceID").getBytes());
				stlPut.add(base.getBytes(), qbase[1].getBytes(), resultSet.getString("Event").getBytes());
				stlPut.add(base.getBytes(), qbase[2].getBytes(), jsonLacation.get("lng").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[3].getBytes(), jsonLacation.get("lat").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[4].getBytes(), jsonResult.get("formatted_address").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[5].getBytes(), jsonResult.get("business").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[6].getBytes(), jsonAddressComponent.get("province").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[7].getBytes(), jsonAddressComponent.get("city").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[8].getBytes(), jsonAddressComponent.get("district").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[9].getBytes(), jsonAddressComponent.get("street").toString().getBytes());
				stlPut.add(base.getBytes(), qbase[10].getBytes(), jsonAddressComponent.get("street_number").toString().getBytes());
				int i=0;
				while (itPois.hasNext()) {
					jsonPoi=(JSONObject)itPois.next();
					stlPut.add(pois.getBytes(), ("poiId"+String.valueOf(i)).getBytes(),jsonPoi.get("uid").toString().getBytes());
					get=new Get(jsonPoi.get("uid").toString().getBytes());
					poiResult=poiTable.get(get);
					if(poiResult.isEmpty())
					{
						Put poiPut=new Put(jsonPoi.get("uid").toString().getBytes());
						poiLacation=jsonPoi.getJSONObject("point");
						poiPut.add(pois.getBytes(), qpois[0].getBytes(), jsonPoi.get("addr").toString().getBytes());
						poiPut.add(pois.getBytes(), qpois[1].getBytes(), jsonPoi.get("name").toString().getBytes());
						poiPut.add(pois.getBytes(), qpois[2].getBytes(), jsonPoi.get("poiType").toString().getBytes());
						poiPut.add(pois.getBytes(), qpois[5].getBytes(), jsonPoi.get("tag").toString().getBytes());
						poiPut.add(pois.getBytes(), qpois[3].getBytes(), poiLacation.get("x").toString().getBytes());
						poiPut.add(pois.getBytes(), qpois[4].getBytes(), poiLacation.get("y").toString().getBytes());
						poiList.add(poiPut);
					}
				}
				stlList.add(stlPut);
				if (stlList.size()>10000) {
					stlTable.put(stlList);
					if(!poiList.isEmpty()){
						poiTable.put(poiList);
					}
					log.debug(tableName + ":completing data copy!");
			        stlList = new ArrayList<Put>();
			        poiList = new ArrayList<Put>();
				}
				}
				else {
					log.error("BaiduMapApi error:"+jsonObject.get("status").toString());
				}
			}
			if (!stlList.isEmpty()) {
				stlTable.put(stlList);
				if(!poiList.isEmpty()){
					poiTable.put(poiList);
				}
			}
			log.debug(tableName + ":completed data copy!");
			stlTable.close();
			poiTable.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			db.close();
		}

	}
	public static void main(String[] args) {
		String hpois="pois";
		String hstl="stl_p1_d";
		String [] stl_columnfamily={"base","pois"};
		String [] pois_columnfamily={"pois"};
		creatHTable(hpois, pois_columnfamily);
	}

}
