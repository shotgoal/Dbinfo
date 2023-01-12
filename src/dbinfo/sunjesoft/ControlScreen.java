package dbinfo.sunjesoft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class ControlScreen {

	static Logger logger = LogManager.getLogger("ControlScreen");
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		// TODO Auto-generated method stub
		// DB 정보 문서와 데이터 이관하는 작업을 관리하는 console형 화면 으로 변경
		start:while(true){
			
		System.out.println("==========================================");
		System.out.println("Data Migration Tool for Oracle->Goldilocks");
		System.out.println("==========================================");
		System.out.println("Press '1' : Create Excel file for Table information on Oracle");
		System.out.println("Press '2' : Create Excel file for Table information on Goldilocks");
		System.out.println("Press '3' : Create SQL for create tables on GOLDILOCKS by Oracle table info");
		System.out.println("Press '4' : Table data transfering from Oracle to Goldilocks which has un-supported data type");
		System.out.println("Press 'z' : exit program!!");
		System.out.println("==========================================");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String command = null;
		try {
			command = reader.readLine();
		}catch (IOException e)
		{
			logger.error("error:",e);
		}
		
		//String command = args[0];
		if(command.trim().equals("1"))
		{
			System.out.println("Choose : 1. Start to job now.");
			//InfoManager im = new InfoManager();
			try {
				String[] args1 = {"ORACLE","1"};
				InfoManager.main(args1);
				/*
				System.out.println("Do you want other job? choose y or n.");
				command = reader.readLine();
				if(command.equals("y"))
				{
					continue start;
				}
				*/
			} catch (Exception e) {
				logger.error("error:",e);
			}
		}
		else if(command.trim().equals("2"))
		{
			//System.out.println("Choose : 2. Start to job now.");
			//InfoManager im = new InfoManager();
			try {
				String[] args1 = {"GOLDILOCKS","2"};
				InfoManager.main(args1);
			} catch (Exception e) {
				logger.error("error:",e);
			}
		}
		else if(command.trim().equals("3"))
		{
			//System.out.println("Choose : 3. Start to job now.");
			//InfoManager im = new InfoManager();
			try {
				String[] args1 = {"ORACLE","3"};
				InfoManager.main(args1);
			} catch (Exception e) {
				logger.error("error:",e);
			}
		}
		else if(command.trim().equals("4"))
		{
			//System.out.println("Choose : 4. Start to job now.");
			//InfoManager im = new InfoManager();
			try {
				ConvertManager.main(null);
			} catch (Exception e) {
				logger.error("error:",e);
			}
			finally
			{
				continue start;
			}
		}
		else if(command.trim().equals("5"))
		{
			//System.out.println("Choose : 4. Start to job now.");
			//InfoManager im = new InfoManager();
			try {
				ConvertManager.main(null);
			} catch (Exception e) {
				logger.error("error:",e);
			}
			finally
			{
				continue start;
			}
		}
		else if(command.trim().toUpperCase().equals("Z"))
		{
			System.out.println("Choose : z. bye");
			System.exit(0);
		}
		
		}
	}

}
