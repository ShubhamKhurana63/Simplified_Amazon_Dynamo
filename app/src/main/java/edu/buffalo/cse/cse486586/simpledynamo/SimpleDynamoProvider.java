package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import java.util.*;
import android.telephony.TelephonyManager;
import android.content.Context;
import java.net.InetAddress;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadPoolExecutor;
import java.net.ServerSocket;

public class SimpleDynamoProvider extends ContentProvider {



	static final String MIN_SHA1="0000000000000000000000000000000000000000";

	static final String MAX_SHA1="ffffffffff"+"ffffffffff"+"ffffffffff"+"ffffffffff";

	List<String> succPredList=new ArrayList<String>();

	Map<String,String> avdHashMap=new HashMap<String, String>();

	Map<String,String> avdPortMap=new HashMap<String, String>();

	static final Integer SERVER_PORT=10000;

	boolean recoveryFlag=false;

	private class ServerTask extends AsyncTask<ServerSocket,Void,Void>
	{
		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			Log.e("Socket Established",">>>>>>>>>>>>>");

				PrintWriter printWriter=null;
				BufferedReader bufferedReader=null;
				while(true)
				{
					try
					{
						Socket socket=serverSocket.accept();//blocking call, where server waits for the client to establish connection & send data
						while(recoveryFlag)
						{
							//Log.e("====","Custom Delay");
						}
						InputStream is=socket.getInputStream();
						OutputStream os=socket.getOutputStream();
						bufferedReader=new BufferedReader(new InputStreamReader(is));
						Log.e("Socket Established",">>>>>>>>>>>>>"+socket.isConnected());
						String data=bufferedReader.readLine();

						if(data!=null && data.contains("Insert:")){
							StringTokenizer tokenizer=new StringTokenizer(data,"|");
							tokenizer.nextToken();
							Log.e("==Data received==",data);
							boolean flag=insertData(tokenizer.nextToken(),tokenizer.nextToken(),getContext(),tokenizer.nextToken(),1);
							Log.e("====Data Inserted====",""+flag);
							printWriter=new PrintWriter(os);
							printWriter.write("inserted-ack"+"\n");
							printWriter.flush();
							printWriter.close();
						}else if(data!=null && data.contains("QueryLocal:"))
						{
							String localDump=getDumpFromLocal(getContext());
							printWriter=new PrintWriter(os);
							printWriter.write(localDump+"\n");
							printWriter.flush();
							printWriter.close();
						}else if(data!=null && data.contains("QuerySelect:"))
						{
							StringTokenizer tokenizer=new StringTokenizer(data,"|");
							tokenizer.nextToken();
							String selection=tokenizer.nextToken();
							Log.e("QUERY SERVER",selection);
							String selectedKeyVal=onSelectionQuery(selection,getContext());
							printWriter=new PrintWriter(os);
							printWriter.write(selectedKeyVal+"\n");
							printWriter.flush();
							printWriter.close();
						}else if(data!=null && data.contains("Delete*:"))
						{
							boolean flag=deleteLocal(getContext());
							Log.e("DELETED *",""+flag);
							printWriter=new PrintWriter(os);
							printWriter.write("deleted-ack"+"\n");
							printWriter.flush();
							printWriter.close();
						}else if(data!=null && data.contains("DeleteSelection:"))
						{
							StringTokenizer tokenizer=new StringTokenizer(data,"|");
							tokenizer.nextToken();
							String selection=tokenizer.nextToken();
							boolean flag=deleteOnSelection(selection,getContext());
							Log.e("DELETED Select",""+flag);
							printWriter=new PrintWriter(os);
							printWriter.write("deleted-ack"+"\n");
							printWriter.flush();
							printWriter.close();
						}else if(data!=null && data.contains("DataUniformity:"))
						{
							StringTokenizer tokenizer=new StringTokenizer(data,"|");
							tokenizer.nextToken();
							String dataFetched=getDataForCoordinator(tokenizer.nextToken(),getContext());
							printWriter=new PrintWriter(os);
							printWriter.write(dataFetched+"\n");
							printWriter.flush();
							printWriter.close();
						}
					}catch(SocketTimeoutException ex)
						{
							//ex.printStackTrace();
							Log.e("==ERROR CAUGHT1SER","Socket Timeout");
						}
					catch(IOException ex)
						{
							//ex.printStackTrace();
							Log.e("==ERROR CAUGHT2SER","Socket Timeout");
						}
				}



			//return null;
		}
	}

	public boolean deleteOnSelection(String selection,Context context)
	{
		boolean flag=false;
		File file=context.getFileStreamPath(selection);

		if(file.exists())
		{
			file.delete();
			flag=true;
		}
		return flag;
	}

	//deleting local apart from the Status File
	public boolean deleteLocal(Context context)
	{
		boolean flag=false;
		File directory=context.getFilesDir();
		if(directory.isDirectory())
		{
			Log.e("<==DEL DIRECTORY==>",""+flag);
			File[] files=directory.listFiles();
			if(files.length>0)
			{
				for(File file:files)
				{
					if(!file.getName().equals(STATUS_FILE_NAME))
					{
						file.delete();
					}
				}
				flag=true;
			}
		}
		Log.e("<==DELETED LOCAL==>",""+flag);
		return flag;
	}



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		// TODO Auto-generated method stub
		while(recoveryFlag)//added for delay while recovery
		{

		}
		Log.e("<==IN DELETE==>",selection);
		String currentPort=getCurrentPortNumber();
		if(selection.equals("*")||selection.equals("@"))
		{
			Context context=getContext();
			boolean flag=deleteLocal(context);
			Log.e("<==DELETED LOCAL==>",""+flag);
			if(selection.equals("*"))
			{
				String toBePassed="Delete*:"+"|";
				for(String port:succPredList)
				{
					if(!currentPort.equals(port))
					{
						toBePassed=toBePassed+port+"@";
					}
				}
				toBePassed=toBePassed.substring(0,toBePassed.length()-1);
				new RollingToSuccessors().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,toBePassed,"deleted-ack","Delete *");
			}
		}else{
			String hashedKey=getHash(selection);
			int nodePortIdx=fetchIndexForNode(hashedKey);
			String toBePassed="DeleteSelection:"+"|"+succPredList.get(nodePortIdx)+"@"+succPredList.get((nodePortIdx+1)%5)+"@"+succPredList.get((nodePortIdx+2)%5)+"|"+selection;
			new RollingToSuccessors().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,toBePassed,"deleted-ack","Delete Selection");
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	//on selection of a particular file
	public String onSelectionQuery(String selection,Context context)
	{
		String finalSel="";
		try
		{
			File file=context.getFileStreamPath(selection);
			if(file.exists())
			{
				FileReader fileReader=new FileReader(file);
				BufferedReader bufferedReader=new BufferedReader(fileReader);
				String value=bufferedReader.readLine();
				StringTokenizer tokenizer=new StringTokenizer(value,"|");
				finalSel=selection+":"+tokenizer.nextToken()+"|"+tokenizer.nextToken();
			}

		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return finalSel;
	}


	// logic which verifies if the hash lies in a particular range
	public boolean verifyRange(String hashOfKey,String predecessorHash,String selfHash)
	{
		int predNum=compareStrings(predecessorHash,hashOfKey);//comparing hash with the
		int selfNum=compareStrings(selfHash,hashOfKey);
		int selfToPred=compareStrings(selfHash,predecessorHash);//same scale

		boolean flag=false;
		if(selfToPred>0 && predNum<0 && selfNum>=0)//checking if the value is in the range
		{
			Log.e("====1====",""+selfToPred);
			return true;
		}

		if(selfToPred<0)
		{
			int minToSelf=compareStrings(selfHash,MIN_SHA1);//successor is always clockwise

			int maxToPred=compareStrings(predecessorHash,MAX_SHA1);//so pred should be behind the max

			if(maxToPred<0 && minToSelf>0)
			{
				//check if it lies between the min and self

				int minToHash=compareStrings(MIN_SHA1,hashOfKey);
				int hashToSelf=compareStrings(hashOfKey,selfHash);

				if(minToHash<=0 && hashToSelf<=0)
				{
					Log.e("====2====",""+selfToPred);
					return true;
				}

				//check if it lies between the predecessor and max
				int maxToHash=compareStrings(MAX_SHA1,hashOfKey);
				int hashToPred=compareStrings(hashOfKey,predecessorHash);

				if(maxToHash>=0 && hashToPred>0)
				{
					Log.e("====3====",""+selfToPred);
					return true;
				}
			}
			Log.e("====4====",""+selfToPred);
		}
		return flag;
	}



	private int compareStrings(String parent,String arg)
	{
		return parent.compareTo(arg);
	}


	private String getHash(String key)
	{
		try
		{
			return genHash(key);
		}catch(NoSuchAlgorithmException ex)
		{
			ex.printStackTrace();
		}
		return null;
	}

	// method for fetching the index of the node in the predefined list
	private int fetchIndexForNode(String hashedKey)
	{
		for(int i=0;i<succPredList.size();i++)
		{
			String self=succPredList.get(i);
			int predIndex=i-1;
			if(i==0)
			{
				predIndex=succPredList.size()-1;
			}
			String pred=succPredList.get(predIndex);

			if(verifyRange(hashedKey,avdHashMap.get(pred),avdHashMap.get(self)))
			{
				return i;
			}
		}
		return -1;
	}

	// method for recovery
	public String getDataForCoordinator(String ports,Context context)
	{
		StringTokenizer portTokenizer=new StringTokenizer(ports,"@");
		Set<String> lookupSet=new HashSet<String>();
		while(portTokenizer.hasMoreTokens())
		{
			lookupSet.add(portTokenizer.nextToken());
		}
		File directory=context.getFilesDir();
		StringBuilder builder=new StringBuilder();
		if(directory.isDirectory())
		{
			File[] files=directory.listFiles();
			if(files.length>0)
			{
				for(File file:files)
				{
					try
					{
						if(!file.getName().equals(STATUS_FILE_NAME)) {
							FileReader fileReader=null;
							try
							{

								fileReader = new FileReader(file);
								BufferedReader bufferedReader = new BufferedReader(fileReader);
								String value = bufferedReader.readLine();
								StringTokenizer tokenizer = new StringTokenizer(value, "|");
								String data = tokenizer.nextToken();
								String version = tokenizer.nextToken();
								String coordinatorId = tokenizer.nextToken();
								if (lookupSet.contains(coordinatorId)) {
									builder.append(file.getName());
									builder.append("|");
									builder.append(data);
									builder.append("|");
									builder.append(version);
									builder.append("|");
									builder.append(coordinatorId);
									builder.append("@");
								}
							}
						finally {
							fileReader.close();
						}
						}
					}catch (Exception ex)
					{
						ex.printStackTrace();
					}
				}
			}
		}
		return builder.toString();
	}



	public String getCurrentPortNumber()
	{
		TelephonyManager tel=(TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Log.e("====PORT====",portStr);
		return portStr;
	}
	// insert data for data recovery
	public  boolean insertDataForRecovery(String fileName,String value,Context context,String clientId,int vectorClock)
	{
		boolean flag=false;
		Log.e("<===FILE STATUS======>",""+context.toString());
		Log.e("==filename==",fileName);
		Log.e("==value==",value);
		Log.e("==clientId==",clientId);

		File file=context.getFileStreamPath(fileName);
		//Log.e("==PATH==",file.getAbsolutePath());
		//Log.d("",context.getFilesDir().toString());
		FileWriter fileWriter=null;
		try {
			try
			{
				Log.e("==PATH1==",file.getAbsolutePath());
				//int vectorClock=1;
				if(!file.exists())
				{
					Log.e("==PATH3==",file.getAbsolutePath());
					file=new File(context.getFilesDir(),fileName);
				}
				fileWriter=new FileWriter(file);
				String toBeWritten=value+"|"+vectorClock+"|"+clientId;
				fileWriter.write(toBeWritten+"\n");
				fileWriter.flush();
				Log.e("==PATH4==",file.getAbsolutePath());
				flag=true;
			}finally {
				//Log.d("<===FILE STATUS======>",""+file.exists());
				fileWriter.close();
			}
		}catch (IOException ex) {
			ex.printStackTrace();
		}
		Log.e("<===FILE STATUS======>",""+flag);
		return flag;
	}


	//insert data
	public  boolean insertData(String fileName,String value,Context context,String clientId,int vectorClock)
	{
		boolean flag=false;
		Log.e("<===FILE STATUS======>",""+context.toString());

		Log.e("==filename==",fileName);
		Log.e("==value==",value);
		Log.e("==clientId==",clientId);

		File file=context.getFileStreamPath(fileName);
		//Log.e("==PATH==",file.getAbsolutePath());
		//Log.d("",context.getFilesDir().toString());
		FileWriter fileWriter=null;
		try {
			try
			{
				Log.e("==PATH1==",file.getAbsolutePath());
				//int vectorClock=1;
				if(file.exists())
				{
					Log.e("==PATH2==",file.getAbsolutePath());
					FileReader fileReader=new FileReader(file);
					BufferedReader bufferedReader=new BufferedReader(fileReader);
					String fileValue=bufferedReader.readLine();
					StringTokenizer tokenizer=new StringTokenizer(fileValue,"|");
					tokenizer.nextToken();
					vectorClock=Integer.parseInt(tokenizer.nextToken())+1;
					clientId=tokenizer.nextToken();
				}else{
					Log.e("==PATH3==",file.getAbsolutePath());
					file=new File(context.getFilesDir(),fileName);
				}
				fileWriter=new FileWriter(file);
				String toBeWritten=value+"|"+vectorClock+"|"+clientId;
				fileWriter.write(toBeWritten+"\n");
				fileWriter.flush();
				Log.e("==PATH4==",file.getAbsolutePath());
				flag=true;
			}finally {
				//Log.d("<===FILE STATUS======>",""+file.exists());
				fileWriter.close();
			}
		}catch (IOException ex) {
			ex.printStackTrace();
		}
		Log.e("<===FILE STATUS======>",""+flag);
		return flag;
	}


	//generic method for conveying the update
	private boolean conveyAVD(String port,String message,String ack,String purpose)
	{
		boolean flag=false;
		try
		{
			Log.e("Inside CAVD",""+purpose);
			Log.e("CAVD PORT",""+port);
			Socket linkingSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			//if(isFailurePossible)
			{
				//linkingSocket.setSoTimeout(500);
			}
			PrintWriter printWriter=new PrintWriter(linkingSocket.getOutputStream());

			printWriter.print(message+"\n");
			printWriter.flush();

			BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(linkingSocket.getInputStream()));

			String receivedAck=bufferedReader.readLine();

			if(receivedAck!=null && receivedAck.equals(ack))
			{
				Log.e("CAVD ACK",""+receivedAck);
				Log.e("CAVD ACK","=====ACKNOWLEDGED=====");
				flag=true;
				Log.e("Include Me",""+flag);
			}
			linkingSocket.close();
		}catch(SocketTimeoutException ex)
		{
			Log.e("==ERROR CAUGHT1","Socket Timeout");
			//ex.printStackTrace();
		}
		catch(IOException ex)
		{
			Log.e("==ERROR CAUGHT2","Socket Timeout");
			//ex.printStackTrace();
		}
		return flag;
	}

	class RollingToSuccessors extends AsyncTask<String, Void, Void>
	{
		@Override
		protected Void doInBackground(String... strings) {
			Log.e("===received===",strings[0]);
			StringTokenizer tokenizer=new StringTokenizer(strings[0],"|");

			String mode=tokenizer.nextToken();

			List<String> portList=new ArrayList<String>();
			String portsString=tokenizer.nextToken();
			StringTokenizer portTokenizer=new StringTokenizer(portsString,"@");
			while(portTokenizer.hasMoreTokens())
			{
				portList.add(portTokenizer.nextToken());
			}

			String value="";
			if(mode.contains("Insert:"))
			{
				String fileName=tokenizer.nextToken();
				String fileValue=tokenizer.nextToken();
				String coordinatorId=tokenizer.nextToken();
				value=mode+"|"+fileName+"|"+fileValue+"|"+ coordinatorId;
			}else if(mode.contains("Delete*:"))
			{
				value=mode;
			}else if(mode.contains("DeleteSelection:"))
			{
				value=mode+"|"+tokenizer.nextToken();
			}
			String ack=strings[1];
			String purpose=strings[2];

			Log.e("==Send String",value);
			for(int i=0;i<portList.size();i++)
			{
				String port=avdPortMap.get(portList.get(i));
				if(!conveyAVD(port,value,ack,purpose))
				{
					Log.e(" PORT FAILED INSERT ",port);
				}else{
					Log.e(" SUCCESS IN INSERT ",port);
				}
			}
			return null;
		}
	}



	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


		while(recoveryFlag)
		{

		}

		Context context=getContext();
		Set<String> keySet=values.keySet();
		Stack<String> stack=new Stack<String>();
		for(String s:keySet)
		{
			stack.push((String)values.get(s));
		}
		String fileName=stack.pop();
		String value=stack.pop();
		String hashedKey=getHash(fileName);

		int nodePortIdx=fetchIndexForNode(hashedKey);
		String coordinatorId=getCurrentPortNumber();//returns avd number

		Log.e("=DETECETD COORDINATOR=",succPredList.get(nodePortIdx));
		String stringToBePassed="Insert:"+"|"+succPredList.get(nodePortIdx)+"@"+succPredList.get((nodePortIdx+1)%5)+"@"+succPredList.get((nodePortIdx+2)%5)+"|"+fileName+"|"+value+"|"+succPredList.get(nodePortIdx);

			//code logic for insert
		new RollingToSuccessors().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,stringToBePassed,"inserted-ack","INSERT");

		return null;
	}
	Map<String,String> nodePredecessorMap=new HashMap<String, String>();
	Map<String,String> nodeSuccessorMap=new HashMap<String, String>();
	public void initialize()
	{
		succPredList.add("5562");
		succPredList.add("5556");
		succPredList.add("5554");
		succPredList.add("5558");
		succPredList.add("5560");
		try
		{
			avdHashMap.put("5554",genHash("5554"));
			avdHashMap.put("5556",genHash("5556"));
			avdHashMap.put("5560",genHash("5560"));
			avdHashMap.put("5558",genHash("5558"));
			avdHashMap.put("5562",genHash("5562"));
		}catch(NoSuchAlgorithmException ex)
		{
			ex.printStackTrace();
		}
		avdPortMap.put("5554","11108");
		avdPortMap.put("5556","11112");
		avdPortMap.put("5558","11116");
		avdPortMap.put("5560","11120");
		avdPortMap.put("5562","11124");

		nodePredecessorMap.put("5554","5556|5562");
		nodePredecessorMap.put("5556","5562|5560");
		nodePredecessorMap.put("5558","5554|5556");
		nodePredecessorMap.put("5560","5558|5554");
		nodePredecessorMap.put("5562","5560|5558");

		nodeSuccessorMap.put("5554","5558|5560");
		nodeSuccessorMap.put("5556","5554|5558");
		nodeSuccessorMap.put("5558","5560|5562");
		nodeSuccessorMap.put("5560","5562|5556");
		nodeSuccessorMap.put("5562","5556|5554");
	}


	class DataObj
	{
		int versionId;
		String value;
		String coordinatorId;

		public DataObj(String value,int versionId,String coordinatorId)
		{
			this.versionId=versionId;
			this.value=value;
			this.coordinatorId=coordinatorId;
		}


		@Override
		public String toString() {
			return versionId+"|"+value+"|"+coordinatorId;
		}
	}

	public void fillMap(String dataReceived,Map<String,DataObj> dataLookUpMap,String toBeContacted)
	{

		StringTokenizer dataTokenizer=new StringTokenizer(dataReceived,"@");
		while(dataTokenizer.hasMoreTokens())
		{
			String data=dataTokenizer.nextToken();
			StringTokenizer fileValueTokenizer=new StringTokenizer(data,"|");
			String fileName=fileValueTokenizer.nextToken();
			String value=fileValueTokenizer.nextToken();
			String version=fileValueTokenizer.nextToken();
			String coordinatorId=fileValueTokenizer.nextToken();
			Log.e("==RECOV DATA=="+toBeContacted+" ",fileName+"|"+value+"|"+version+"|"+coordinatorId);
			if(dataLookUpMap.get(fileName)==null)
			{
				dataLookUpMap.put(fileName,new DataObj(value,Integer.parseInt(version),coordinatorId));
			}else if(dataLookUpMap.get(fileName)!=null)
			{
				DataObj obj=dataLookUpMap.get(fileName);
				if(obj.versionId<Integer.parseInt(version))
				{
					dataLookUpMap.put(fileName,new DataObj(value,Integer.parseInt(version),coordinatorId));
				}
			}
		}
	}

	 class  DataUniform extends AsyncTask<String,Void,Void>
	{
		@Override
		protected Void doInBackground(String... strings) {
			String currentPort=strings[0];
			Map<String,String> portsCoordinatorId=new HashMap<String, String>();
			String predStr=nodePredecessorMap.get(currentPort);
			StringTokenizer tokenizer=new StringTokenizer(predStr,"|");

			String predecessor1=tokenizer.nextToken();
			String predecessor2=tokenizer.nextToken();

			String succStr=nodeSuccessorMap.get(currentPort);
			StringTokenizer tokenizer2=new StringTokenizer(succStr,"|");

			String successor1=tokenizer2.nextToken();
			String successor2=tokenizer2.nextToken();

			portsCoordinatorId.put(predecessor1,predecessor1+"@"+predecessor2);
			portsCoordinatorId.put(predecessor2,predecessor2);
			portsCoordinatorId.put(successor1,currentPort+"@"+predecessor1);
			portsCoordinatorId.put(successor2,currentPort);

			Map<String,DataObj> dataLookUpMap=new HashMap<String, DataObj>();
			for(String toBeContacted:portsCoordinatorId.keySet())
			{
				try
				{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(avdPortMap.get(toBeContacted)));
					PrintWriter printWriter=new PrintWriter(socket.getOutputStream());
					printWriter.print("DataUniformity:"+"|"+portsCoordinatorId.get(toBeContacted)+"\n");
					printWriter.flush();
					BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String dataReceived=bufferedReader.readLine();
					if(dataReceived!=null && dataReceived.length()>0)
					{
						fillMap(dataReceived,dataLookUpMap,toBeContacted);
					}
					socket.close();
				}catch(SocketTimeoutException ex)
				{
					ex.printStackTrace();
				}catch(IOException ex)
				{
					ex.printStackTrace();
				}
			}
			for(String key:dataLookUpMap.keySet())
			{
				DataObj obj=dataLookUpMap.get(key);
				Log.e("RECOVERY"+key,obj.toString());
				insertDataForRecovery(key,obj.value,getContext(),obj.coordinatorId,obj.versionId);
			}
			recoveryFlag=false;
			return null;
		}
	}

	String STATUS_FILE_NAME="Status_File";

	private boolean allreadyUpStatus(Context context)
	{
		boolean flag=true;
		//String  fileName=;
		File file = context.getFileStreamPath(STATUS_FILE_NAME);
		if(!file.exists())
		{
			flag=false;
			insertData(STATUS_FILE_NAME,"upstatus",context,"NA",0);
		}
		return flag;
	}

boolean isFailurePossible=false;

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		initialize();
		try
		{
			ServerSocket serverSocket=new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
			//deleting all the local files
			if(allreadyUpStatus(getContext()))
			{
				recoveryFlag=true;
				Log.e("ENTERING FOR RECOV",""+isFailurePossible);
				isFailurePossible=true;
				String currentPort=getCurrentPortNumber();
				deleteLocal(getContext());
				new DataUniform().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,currentPort);
				//Thread.sleep(100);
			}
		}catch(IOException ex)
		{

		}
		return false;
	}

	public String getDumpFromLocal(Context context)
	{
		File directory=context.getFilesDir();
		StringBuilder builder=new StringBuilder();
		if(directory.isDirectory())
		{
			File[] files=directory.listFiles();
			if(files.length>0)
			{
				for(File file:files)
				{//change

					if(!file.getName().equals("Status_File"))
					{
						try
						{
							FileReader fileReader=null;
							try
							{
								fileReader=new FileReader(file);
								BufferedReader bufferedReader=new BufferedReader(fileReader);
								String value=bufferedReader.readLine();
								builder.append(file.getName());
								builder.append(":");
								StringTokenizer tokenizer=new StringTokenizer(value,"|");
								builder.append(tokenizer.nextToken());
								builder.append("|");
							}finally {
								fileReader.close();
							}
						}catch (Exception ex)
						{
							ex.printStackTrace();
						}
					}
				}
			}
		}
		return builder.toString();
	}


	private MatrixCursor parseData(String dump)
	{
		String [] colNames= {"key","value"};
		MatrixCursor matrixCursor=new MatrixCursor(colNames);
		if(dump!=null&& dump.length()>0)
		{
			StringTokenizer tokenizer=new StringTokenizer(dump,"|");
			while(tokenizer.hasMoreTokens())
			{
				StringTokenizer colonTokenizer=new StringTokenizer(tokenizer.nextToken(),":");
				if(colonTokenizer!=null)
				{
					String[] array=new String[2];
					array[0]=colonTokenizer.nextToken();
					array[1]=colonTokenizer.nextToken();
					matrixCursor.addRow(array);
				}
			}
		}
		return matrixCursor;
	}


		class LocalQueryTask extends AsyncTask<String,Void,Void>
		{

			@Override
			protected Void doInBackground(String... strings) {
				//reconcile

				String selection=strings[0];
				Log.e("==QUERY FOR==",selection);
				String toBeUsedPort=strings[1];
				String currentPort=getCurrentPortNumber();

				String fetchedDump="";
				if(selection.equals("@"))
				{
					String localDumpString=getDumpFromLocal(getContext());
					localQueryDelegate=localDumpString;
				}else if(selection.equals("*"))
				{
					for(int i=0;i<succPredList.size();i++)
					{
						if(!succPredList.get(i).equals(toBeUsedPort))
						{
							try
							{
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(avdPortMap.get(succPredList.get(i))));
								//if(isFailurePossible)
								{
									//socket.setSoTimeout(600);
								}
								PrintWriter printWriter=new PrintWriter(socket.getOutputStream());
								printWriter.print("QueryLocal:"+"\n");
								printWriter.flush();
								BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
								String tempDump=bufferedReader.readLine();
								if(tempDump!=null && tempDump.length()>0)
								{
									fetchedDump=fetchedDump+tempDump;
								}
								socket.close();
							}catch(SocketTimeoutException ex)
							{
								Log.e("==ERROR CAUGHT1","Socket Timeout");
								//ex.printStackTrace();
							}catch(IOException ex)
							{
								Log.e("==ERROR CAUGHT2","Socket Timeout");
								//ex.printStackTrace();
							}

						}
					}String localDumpString=getDumpFromLocal(getContext());
				localQueryDelegate=localDumpString+fetchedDump;
				}else{
                    StringTokenizer tokenizer=new StringTokenizer(toBeUsedPort,"@");
                    List<String> portsList=new ArrayList<String>();
                    while(tokenizer.hasMoreTokens())
                    {
                        portsList.add(tokenizer.nextToken());
                    }
                    int maxVersion=0;
                    String respDump="";
                    for(String port:portsList)
                    {
                        try
                        {
                        	if(port.equals(currentPort))
							{
								fetchedDump=onSelectionQuery(selection,getContext());
								if(fetchedDump!=null && fetchedDump.length()>0)
								{
									StringTokenizer respTokenizer=new StringTokenizer(fetchedDump,"|");
									fetchedDump=respTokenizer.nextToken();
									int versionFetched=Integer.parseInt(respTokenizer.nextToken());
									if(versionFetched>maxVersion)
									{
										respDump=fetchedDump;
										maxVersion=versionFetched;
									}
								}

								continue;
							}
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(avdPortMap.get(port)));
                            //if(isFailurePossible)
							{
								//socket.setSoTimeout(600);
							}
							//Log.e(" FOR SELECTION",selection);
                            PrintWriter printWriter=new PrintWriter(socket.getOutputStream());
                            printWriter.print("QuerySelect:"+"|"+selection+"\n");
                            printWriter.flush();
                            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            fetchedDump=bufferedReader.readLine();
                            if(fetchedDump!=null && fetchedDump.length()>0)
							{
								StringTokenizer respTokenizer=new StringTokenizer(fetchedDump,"|");
								fetchedDump=respTokenizer.nextToken();
								int versionFetched=Integer.parseInt(respTokenizer.nextToken());
								//Log.e("VERSION",""+versionFetched);
								Log.e("FETCHED",fetchedDump);
								if(versionFetched>maxVersion)
								{
									respDump=fetchedDump;
									maxVersion=versionFetched;
								}
								Log.e("==CUrr VERSION==",""+versionFetched);
							}
                            socket.close();
                        }catch(SocketTimeoutException ex)
                        {
							Log.e("==ERROR CAUGHT1","Socket Timeout");
                            //ex.printStackTrace();
                        }catch(IOException ex)
                        {
							Log.e("==ERROR CAUGHT2","Socket Timeout");
                            //ex.printStackTrace();
                        }
                    }
					Log.e("QUERIED",respDump);
                    localQueryDelegate=respDump;
				}
				return null;
			}
		}

		static String localQueryDelegate;
		public synchronized  String localQueryHelper(String selection,String currentPort)
		{
			 new LocalQueryTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,selection,currentPort);
			 String data="";
			 while(true)
			 {
			 	if(localQueryDelegate!=null)
				{
					data=localQueryDelegate;
					localQueryDelegate=null;
					break;
				}
			 }
			return data;
		}





	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {


			while(recoveryFlag)
			{

			}



		// TODO Auto-generated method stub
		Context context=getContext();
		String [] colNames= {"key","value"};
		MatrixCursor matrixCursor=new MatrixCursor(colNames);
		String currentPort=getCurrentPortNumber();
		if(selection.equals("@") || selection.equals("*"))
		{
			String dump= localQueryHelper(selection,currentPort);
			return parseData(dump);
		}else{
			File file=context.getFileStreamPath(selection);
			FileReader fileReader=null;
			Log.e("to be query", selection);


			if(matrixCursor.getCount()==0)
			{
				String hashedKey=getHash(selection);
				int idx=fetchIndexForNode(hashedKey);
				String data=localQueryHelper(selection,succPredList.get(idx)+"@"+succPredList.get((idx+1)%5)+"@"+succPredList.get((idx+2)%5));
				return parseData(data);
			}
		}

		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
