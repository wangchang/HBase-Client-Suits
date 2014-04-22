import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class HbaseSuit {

	public static void main(String[] args) throws Throwable {
		
		String option;
		int chose1;//第一层选择
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		HbaseClient client = new HbaseClient();
		
		while(true){
			HbaseSuit.PrintUsage();
			option = br.readLine();
			chose1 = Integer.parseInt(option);
			if(1 == chose1){
				//TODO无法选择为哪些列做索引！
				System.out.println("\n[INFO]Create a new Hbase Indexed Table,Fellow The Orders:");
				System.out.print("\n***Input Table Name:");
				String tablename = br.readLine();		
				client.setTableName(tablename);
				
				if(client.isTableAvaiable()){
					//表已经存在，显示信息
					System.out.println(String.format("\n[INFO]Table:%s Exists!",tablename));
					client.showTableDescription();
				}
				else{
					//表不存在，可以继续建立表了
					//首先给出列簇
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
					//表已经存在，显示信息
					System.out.println(String.format("\n[INFO]Table:%s Exists!",tablename));
					client.showTableDescription();
					continue;
				}
				else{
					//表不存在，可以继续建立表了
					//首先给出列簇
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
				//用户输入表名
				System.out.print("\n***Input Table Name:");
				String tablename = br.readLine();
				client.setTableName(tablename);
				
				if(client.isTableAvaiable()){
					//返回表结构
					System.out.println("\n[INFO]Show Table Construct(Careful about which column are indexed):");
					client.showTableDescription();	
					
					//用户输入TXT文件目录
					System.out.print("\n***Input TXT File Path:");
					String file = br.readLine();
					
					//显示文件的前几行内容，好让用户设置行键等规则！
					System.out.println("\n[INFO]Show TXT File Construct:");
					if(!client.showFileBase(file))
						continue;
					
					//用户定义行键结构
					System.out.print("\n***Define Row Key Construct\n"
							+ "(LIKE:1,3,5 means Row Key=value1+value3+value5):");
					String rowCons = br.readLine();
					
					//用户定义列名及列值结构
					System.out.print("\n***Define Columns Construct\n"
					+"(LIKE:family:qualifier1:1,family:qualifier2:2):");
					String columnCons = br.readLine();
					
	/*				System.out.print("***Define Write Threads Num:\n");
					String threadNum = br.readLine();*/
					
					//选择文档分隔符
					System.out.print("\n***Define Split Char:");
					String split = br.readLine();
					
					//开始执行导入过程
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
