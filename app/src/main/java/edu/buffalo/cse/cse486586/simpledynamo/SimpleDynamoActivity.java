package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SimpleDynamoActivity extends Activity {


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}



	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		findViewById(R.id.button1).setOnClickListener(
				new View.OnClickListener() {
					@Override
					public void onClick(View v) {

						Uri mUri= mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
						ContentResolver mContentResolver=getContentResolver();

						//Cursor resultCursor=mContentResolver.query(mUri, null, "OHhyMqFH9ermKb1qJHqgf11bA62jcnIY", null, null);


						SimpleDynamoProvider simpleDynamoProvider=new SimpleDynamoProvider();

						//String response=simpleDynamoProvider.onSelectionQuery("",getApplicationContext());
						File file=getApplicationContext().getFileStreamPath("FiFgRjcv44wx9msJ6yyo0RP136FkY57b");

						try
						{
							FileReader fileReader=new FileReader(file);
							BufferedReader bufferedReader=new BufferedReader(fileReader);
							String stry="";
							while(( stry=bufferedReader.readLine())!=null)
							{
								Log.e("==FETCHED==",stry);

							}
						}catch(FileNotFoundException ex)
						{

						}catch(IOException ex)
						{

						}


						//Cursor resultCursor=mContentResolver.query(mUri, null, "*", null, null);
//						ContentValues contentValues=new ContentValues();
//						contentValues.put("key","5558");
//						contentValues.put("values","the value");
						//mContentResolver.insert(mUri,contentValues);
					}
				}
		);














	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
