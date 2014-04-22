import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class HbaseSuit {

	public static void main(String[] args) throws Throwable {
		
		String option;
		int chose1;//��һ��ѡ��
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		HbaseClient client = new HbaseClient();
		
		while(true){
			HbaseSuit.PrintUsage();
			option = br.readLine();
			chose1 = Integer.parseInt(option);
			if(1 == chose1){
				//TODO�޷�ѡ��Ϊ��Щ����������
				System.out.println("\n[INFO]Create a new Hbase Indexed Table,Fellow The Orders:");
				System.out.print("\n***Input Table Name:");
				String tablename = br.readLine();		
				client.setTableName(tablename);
				
				if(client.isTableAvaiable()){
					//���Ѿ����ڣ���ʾ��Ϣ
					System.out.println(String.format("\n[INFO]Table:%s Exists!",tablename));
					client.showTableDescription();
				}
				else{
					//�����ڣ����Լ�����������
					//���ȸ����д�
					System.out.print("\n***Input Column Families[LIKE:cf1(Only Support One Families)]:");
					String cfs = br.readLine();
					
					System.out.print("\n***Input Index Name[LIKE:field1(Only Support One Value!)]:");
					String indexname = br.readLine();
					
					System.out.print("\n***Input Indexed Column Names\n"
							+ "******[LIKE:c1,c2] Remered Column Names When You Add Records:");
					String indexcolumns = br.readLine();
					
					client.createIndexedTable(tablename,cfs,indexname,indexcolumns);
					client.showTableDescription();
				}			
			}
			else if(2 == chose1){
				System.out.println("\n[INFO]Create a new Hbase Normal Table,Fellow The Orders:");
				System.out.print("\n***Input Table Name:");
				String tablename = br.readLine();		
				client.setTableName(tablename);
				
				if(client.isTableAvaiable()){
					//���Ѿ����ڣ���ʾ��Ϣ
					System.out.println(String.format("\n[INFO]Table:%s Exists!",tablename));
					client.showTableDescription();
					continue;
				}
				else{
					//�����ڣ����Լ�����������
					//���ȸ����д�
					System.out.print("\n***Input Column Families[LIKE:cf1,cf2,...]:");
					String cfs = br.readLine();
					
					if(client.createNormalTable(tablename,cfs))
					         client.showTableDescription();
					else
						continue;
				}			
			}
			else if(3 == chose1){
				System.out.println("\n[INFO]Import TXT Into Hbase Indexed Table,Fellow The Orders:");
				//�û��������
				System.out.print("\n***Input Table Name:");
				String tablename = br.readLine();
				client.setTableName(tablename);
				
				if(client.isTableAvaiable()){
					//���ر�ṹ
					System.out.println("\n[INFO]Show Table Construct(Careful about which column are indexed):");
					client.showTableDescription();	
					
					//�û�����TXT�ļ�Ŀ¼
					System.out.print("\n***Input TXT File Path:");
					String file = br.readLine();
					
					//��ʾ�ļ���ǰ�������ݣ������û������м��ȹ���
					System.out.println("\n[INFO]Show TXT File Construct:");
					if(!client.showFileBase(file))
						continue;
					
					//�û������м��ṹ
					System.out.print("\n***Define Row Key Construct\n"
							+ "(LIKE:1,3,5 means Row Key=value1+value3+value5):");
					String rowCons = br.readLine();
					
					//�û�������������ֵ�ṹ
					System.out.print("\n***Define Columns Construct\n"
					+"(LIKE:family:qualifier1:1,family:qualifier2:2):");
					String columnCons = br.readLine();
					
	/*				System.out.print("***Define Write Threads Num:\n");
					String threadNum = br.readLine();*/
					
					//ѡ���ĵ��ָ���
					System.out.print("\n***Define Split Char:");
					String split = br.readLine();
					
					//��ʼִ�е������
					try{
						client.importTxtToTable(file,tablename,rowCons,columnCons,split);
					}catch(IOException e){
						System.out.println("[ERROR]File Read Wrong!");
						continue;
					}
					catch(InterruptedException e){
						System.out.println("[ERROR]Table Flush Wrong!");
						continue;
					}		
				}
				else
					System.out.println(String.format("\n[INFO]Table:%s Not Exists,Please Create Table First!",tablename));				
			}
			else if(4 == chose1){
				System.out.println("\n[INFO]Based on Colum Value,Use Scan And Filter To Catch A Record");
				System.out.print("\n***Input Table Name:");
				
				String tablename = br.readLine();
				client.setTableName(tablename);
				if(client.isTableAvaiable()){
					client.showTableDescription();
					System.out.println("\n[INFO] Search Format:Column Family:Cloumn Name:value\n"
					+"[INFO]LIKE info:c1:value1,info:c2:value2\n"
							+"[INFO]ONLY SUPPORT SINGLE VALUE!");
					System.out.print("\n***Input Search Args:");
					
					String scanArgs = br.readLine();
					client.scanFilterColumn(scanArgs);
				}
				else
					System.out.println(String.format("\n[INFO]Table:%s Not Exists,Please Create Table First!",tablename));							
			}
			else if(5 == chose1){
				System.out.println("\nBased on Colum Value,Use Intel Hbase Index To Catch A Record");
				System.out.print("\n***Input Table Name:");
				String tablename = br.readLine();
				client.setTableName(tablename);
				if(client.isTableAvaiable()){
					client.showTableDescription();
					System.out.print("\n***Input Index Name:");
					String indexname = br.readLine();
					
					System.out.println("\n[INFO] Search Format:Column Family:Cloumn Name:value\n"
					+"[INFO]LIKE info:c1:value1,info:c2:value2\n"
							+"[INFO]ONLY SUPPORT SINGLE VALUE!");
					System.out.print("\n***Input Search Args:");					
					String scanArgs = br.readLine();
					client.indexFilterColumn(indexname,scanArgs);	
				}
				else
					break;														 
			}
			else if(0 == chose1){
				client.FreeClientResource();
				br.close();
				System.exit(0);
			}
			else{
				System.out.println("\nNo Defined!Please Look at the Useage!!!");
			}
			
		}
	}

	static void PrintUsage(){
    	System.out.print("\n\n*************Welcome To Indexed Hbase Suits***************\n");
    	System.out.println("***Create A Indexed HBase Table : 1");
    	System.out.println("***Create A Normal HBase Table : 2");
    	System.out.println("***Import TXT File Into Hbase Table : 3");
    	System.out.println("***Scan Filter one Column Value:4");
    	System.out.println("***Index Search one Column Value:5");
    	System.out.println("***To Quit : 0");
    	System.out.print("**********************************************2014-04-22****\n\n");
    	System.out.print("***Your Option:");
    }
}
