import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class NRD {
	
	public Connection con;
	public Connection insertcon;
    ListMultimap<String, TransactionData> hashMap = ArrayListMultimap.create();
    public ArrayList<ProductMasterData> productMasterData=new ArrayList<ProductMasterData>();
    public ArrayList<CustomerMasterData> customerMasterData=new ArrayList<CustomerMasterData>();
    Queue<ArrayList<QueueList>> pointersQueue = new LinkedList<ArrayList<QueueList>>();
	public NRD() throws ClassNotFoundException, SQLException
	{
		int TransactionsTotal=0;
		Class.forName("com.mysql.cj.jdbc.Driver");
		String root,pass,name,name2;
		Scanner sc1= new Scanner(System.in); 
		Scanner sc2= new Scanner(System.in); 
		Scanner sc3= new Scanner(System.in); 
		System.out.println("Enter root name: ");
		root=sc1.next();
		System.out.println("Enter Password: ");
		pass=sc2.next();
		System.out.println("Enter source db name: ");
		name=sc3.next();
		
		System.out.println("Enter dest db name: ");
		name2=sc3.next();
		
		
		
		con=DriverManager.getConnection("jdbc:mysql://127.0.0.1:3308/"+name,root,pass);
		insertcon=DriverManager.getConnection("jdbc:mysql://127.0.0.1:3308/"+name2,root,pass);
		Statement stmt=con.createStatement();
		if(con != null && insertcon!=null) {
			System.out.println("Connected");
	        String sql=("Select count(*) from transactions");
	        PreparedStatement pstmt = con.prepareStatement(sql);
	        ResultSet rs = pstmt.executeQuery();
	        if (rs.next())
	        	TransactionsTotal=rs.getInt(1);
		}
		System.out.println(TransactionsTotal);
	}
	
    public void addSupplier(String id, String name) throws SQLException{
        String sql1=("INSERT INTO supplier(supplier_id,supplier_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as supplier_id, ? as supplier_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT supplier_id FROM supplier WHERE supplier_id = ?\n" +
                "    );");
        PreparedStatement pstmt = this.insertcon.prepareStatement(sql1);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void addCustomer(String id, String name) throws SQLException{
        String sql2=("INSERT INTO customer(customer_id,customer_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as customer_id, ? as customer_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT customer_id FROM customer WHERE customer_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.insertcon.prepareStatement(sql2);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void addStore(String id, String name) throws SQLException{
        String sql3=("INSERT INTO store(store_id,store_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as store_id, ? as store_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT store_id FROM store WHERE store_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.insertcon.prepareStatement(sql3);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void addProduct(String id, String name) throws SQLException{
        String sql4=("INSERT INTO product(product_id,product_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as product_id, ? as product_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT product_id FROM product WHERE product_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.insertcon.prepareStatement(sql4);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();

    }

    @SuppressWarnings("deprecation")
	public void addDate(Date date) throws SQLException {
        String sql5=("INSERT INTO date(date,day, month, quater, year)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as date, ? as day, ? as month, ? as quater, ? as year) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT date FROM date WHERE date = ?\n" +
                "    );");
        PreparedStatement pstmt=this.insertcon.prepareStatement(sql5);
        pstmt.setDate(1,  (java.sql.Date) date);
        String day= new SimpleDateFormat("EEEE", Locale.ENGLISH).format(date);
        pstmt.setString(2, day);
        pstmt.setInt(3, date.getMonth()+1);
        int q;
        if (date.getMonth()+1 <=3)
            q=1;
        else if (date.getMonth()+1 <=6)
            q=2;
        else if (date.getMonth()+1 <=9)
            q=3;
        else
            q=4;
        pstmt.setInt(4, q);
        int year=date.getYear()+1900;
        pstmt.setInt(5, year);
        pstmt.setDate(6, (java.sql.Date) date);
        pstmt.executeUpdate();
    }

    public void addFact(int tid, String pid, String cid, String sid, Date date, String suid, int q, double sale) throws SQLException{
        String sql6=("INSERT INTO facttable value (?, ?, ?, ?, ?, ?, ?, ?);");
        PreparedStatement pstmt=this.insertcon.prepareStatement(sql6);
        pstmt.setInt(1, tid);
        pstmt.setString(2, pid);
        pstmt.setString(3, cid);
        pstmt.setString(4, sid);
        pstmt.setDate(5, (java.sql.Date) date);
        pstmt.setString(6,suid);
        pstmt.setInt(7,q);
        pstmt.setDouble(8,sale);
        pstmt.executeUpdate();
       
        System.out.println("Rows added: "+"*******************");
    }

    public void loadTuple(TransactionData tuple) throws SQLException {
        //checking supplier dimension
        this.addSupplier(tuple.Supplier_ID, tuple.Supplier_Name);

        //checking customer dimension
        this.addCustomer(tuple.Customer_ID, tuple.Customer_Name);

        //checking store dimension
        this.addStore(tuple.Store_ID, tuple.Store_Name);

        //checking product dimension
        this.addProduct(tuple.Product_ID,tuple.Product_Name);

        //checking date dimension
        this.addDate((Date) tuple.T_Date);

        //inserting into fact table
        this.addFact(tuple.Transaction_ID,tuple.Product_ID,tuple.Customer_ID, tuple.Store_ID,(Date) tuple.T_Date, tuple.Supplier_ID, tuple.Quantity,tuple.Total_Sale) ;

    }
	
	
	public int loadTransactionData(int index) throws SQLException {
		
		 ArrayList<QueueList>tempArray=new ArrayList<QueueList>();
		 String sql = ("SELECT * FROM transactions where TRANSACTION_ID > ? ORDER BY TRANSACTION_ID ASC LIMIT ?;");
	        PreparedStatement pstmt = con.prepareStatement(sql);
	        pstmt.setInt(1, index);
	        pstmt.setInt(2, 50);
	        ResultSet rs = pstmt.executeQuery();
	        int latest = 0;
	        int c=0;
	        while(rs.next()) {
	        	//Map<String, TransactionData> hashMap = new LinkedHashMap<>();
	            latest = rs.getInt("TRANSACTION_ID");
	            TransactionData td=new TransactionData();
	            td.Transaction_ID=latest;
	            td.Product_ID=rs.getString("PRODUCT_ID");
	            td.Customer_ID=rs.getString("CUSTOMER_ID");
	            td.Store_ID=rs.getString("STORE_ID");
	            td.Store_Name=rs.getString("STORE_NAME");
	            td.T_Date=rs.getDate("T_DATE");
	            td.Quantity=rs.getInt("QUANTITY");
	            this.hashMap.put(td.Product_ID, td);
	            tempArray.add(new QueueList(td.Product_ID,td.Customer_ID));
	            
	            c++;
	        }
	        
	    //System.out.println(this.pointersQueue.size());
	    this.pointersQueue.add(tempArray);
		return latest;
		
		
	}
	
	
	public void loadProductMasterData(int index) throws SQLException {
		
		this.productMasterData.clear();
		String sql = ("SELECT a.PRODUCT_ID, a.PRODUCT_NAME, a.SUPPLIER_ID, a.SUPPLIER_NAME, a.PRICE FROM\n" +
                "    (SELECT *, ROW_NUMBER() OVER ( ORDER BY PRODUCT_ID ) row_num FROM  products) as a\n" +
                "WHERE a.row_num>?\n" +
                "LIMIT 10;");
        PreparedStatement pstmt = con.prepareStatement(sql);
        pstmt.setInt(1, index);
        ResultSet rs = pstmt.executeQuery();
	        
	        this.productMasterData.clear();
	        Statement st = con.createStatement();
	        while(rs.next()) {
	            ProductMasterData temp = new ProductMasterData();
	            temp.PRODUCT_ID=rs.getString(1);
	            temp.PRODUCT_NAME=rs.getString(2);
	            temp.SUPPLIER_ID=rs.getString(3);
	            temp.SUPPLIER_NAME=rs.getString(4);
	            temp.PRICE=rs.getDouble(5);
	            this.productMasterData.add(temp);
	        }
		
		
		
	}
	public void displayTuple(TransactionData td) {
    	System.out.println("TransactionID: "+td.Transaction_ID);
    	System.out.println("Product_ID: "+td.Product_ID);
    	System.out.println("Customer_ID: "+td.Customer_ID);
    	System.out.println("Customer_name: "+td.Customer_Name);
    	System.out.println("Store_ID: "+td.Store_ID);
    	System.out.println("Store_Name: "+td.Store_Name);
    	System.out.println("T_Date: "+td.T_Date);
    	System.out.println("Quantity: "+td.Quantity);
    	System.out.println("ProductName: "+td.Product_Name);
    	System.out.println("SupplierID: "+td.Supplier_ID);
    	System.out.println("SupplierName: "+td.Supplier_Name);
    	System.out.println("Total Sale: "+td.Total_Sale);
    	System.out.println();
    	System.out.println();
	}
	public void displayProduct(ProductMasterData td) {
    	System.out.println("Product_ID: "+td.PRODUCT_ID);
    	System.out.println("ProductName: "+td.PRODUCT_NAME);
    	System.out.println("SupplierID: "+td.SUPPLIER_ID);
    	System.out.println("SupplierName: "+td.SUPPLIER_NAME);
    	System.out.println();
    	System.out.println();
	}
	public void loadCustomerMasterData(int index) throws SQLException {
		
		 
		String sql = ("SELECT a.CUSTOMER_NAME,a.CUSTOMER_ID FROM (SELECT *, ROW_NUMBER() OVER ( ORDER BY CUSTOMER_ID ) row_num FROM  db.customers) as a\r\n"
		 		+ "                WHERE a.row_num>?\r\n"
		 		+ "                LIMIT 50;");
	        PreparedStatement pstmt = con.prepareStatement(sql);
	        pstmt.setInt(1, index);
	        ResultSet rs = pstmt.executeQuery();
	        
	        this.customerMasterData.clear();
	        Statement st = con.createStatement();
	        while(rs.next()) {
	            CustomerMasterData temp = new CustomerMasterData();
	            temp.CUSTOMER_ID=rs.getString(2);
	            temp.CUSTOMERNAME=rs.getString(1);
	            this.customerMasterData.add(temp);
	        }
		
		
		
	}
    public void removeQueueHead() throws SQLException{
        ArrayList<QueueList> toRemove = this.pointersQueue.remove();

        for (QueueList id : toRemove) {
            ArrayList<TransactionData> toDelete = new ArrayList<>();
            Collection<TransactionData> matched = this.hashMap.get(id.ProductID);
            for (TransactionData t : matched) {
                if (t.Product_Name != null) {
                    toDelete.add(t);
                }
            }
            this.hashMap.get(id.ProductID).removeAll(toDelete);
        }
        
        
    }
	public void MeshJoin() throws SQLException {
		System.out.print("hahahahaahahhshahsahs\n");
		int index=0;
		int j=0;
		int p=0;
		int temp=0;
		for(int i=0; i<10000;j+=10, p+=50) {
			
			i=this.loadTransactionData(i);
			//System.out.println("Queue Remaining size is : "+this.pointersQueue.size());
			
			if(j==100)
			{
				j=0;
			}
			if(p==50)
			{
				p=0;
			}
			this.loadCustomerMasterData(p);
			this.loadProductMasterData(j);
			
			for(int x=0; x<10; x++)
			{
				ProductMasterData product=this.productMasterData.get(x);
				//this.displayProduct(product);
				List<TransactionData>maptoMatch=this.hashMap.get(product.PRODUCT_ID);
				//System.out.println("MaptoMatch size: " + maptoMatch.size());
				if(maptoMatch!=null)
				{
					for(int a=0; a<maptoMatch.size(); a++)
					{
						boolean check=false;
						maptoMatch.get(a).Product_Name= product.PRODUCT_NAME;
						maptoMatch.get(a).Supplier_ID= product.SUPPLIER_ID;
						maptoMatch.get(a).Supplier_Name= product.SUPPLIER_NAME;
						maptoMatch.get(a).Total_Sale=maptoMatch.get(a).Quantity*product.PRICE;
							for(int u=0; u<50; u++)
							{
								
								CustomerMasterData customer=this.customerMasterData.get(u);
								if(customer!=null)
								{
									//System.out.println(maptoMatch.get(a).Customer_ID+" = "+customer.CUSTOMER_ID);
									if(maptoMatch.get(a).Customer_ID.equalsIgnoreCase(customer.CUSTOMER_ID))
									{
										System.out.println(index+" Customer name: "+maptoMatch.get(a).Customer_Name);
										//System.out.println("Customer name: "+customer.CUSTOMERNAME);
										maptoMatch.get(a).Customer_Name=customer.CUSTOMERNAME;
										check=true;
										this.loadTuple(maptoMatch.get(a));
	//									this.displayTuple(maptoMatch.get(a));
									}
									else {
										//System.out.println("couldn't match");
									}
								}
								else {
									System.out.println("is null");
								}
							}
							if(check==false) {
							//	System.out.printl("ohoo not matched");
							//	this.loadTuple(maptoMatch.get(a));
	//							this.displayTuple(maptoMatch.get(a));
								
							}
							else {
								check=false;
							}
							System.out.println();
						//	this.displayTuple(maptoMatch.get(a));
						}
					
				}
				
			}
			
            //6. Once all MasterData tuples checked, remove last element in queue
            if (this.pointersQueue.size()==10) {
                this.removeQueueHead();
            }
        }
        //empty the remaining queue
        for (int i=0; i<9; i++, j+=10,p+=50)
        {
        	//System.out.println("Queue Remaining size is : "+this.pointersQueue.size());
            if (j==100){
                j=0;
            }
            if(p==50)
            {
            	p=0;
            }
           this.loadProductMasterData(j);
           this.loadCustomerMasterData(p);
            for(int x=0; x<10; x++)
			{
            	
            	
				ProductMasterData product=this.productMasterData.get(x);
			//	this.displayProduct(product);
				List<TransactionData>maptoMatch=this.hashMap.get(product.PRODUCT_ID);
				//System.out.println("MaptoMatch size: " + maptoMatch.size());
				if(maptoMatch!=null)
				{
					for(int a=0; a<maptoMatch.size(); a++)
					{
						boolean check=false;
						maptoMatch.get(a).Product_Name= product.PRODUCT_NAME;
						maptoMatch.get(a).Supplier_ID= product.SUPPLIER_ID;
						maptoMatch.get(a).Supplier_Name= product.SUPPLIER_NAME;
						maptoMatch.get(a).Total_Sale=maptoMatch.get(a).Quantity*product.PRICE;
						
						for(int u=0; u<50; u++)
						{
							
							CustomerMasterData customer=this.customerMasterData.get(u);
							if(customer!=null)
							{
								System.out.println(index+" Customer name: "+maptoMatch.get(a).Customer_Name);
								if(maptoMatch.get(a).Customer_ID.equalsIgnoreCase(customer.CUSTOMER_ID))
								{
//									System.out.println("Customer name: "+maptoMatch.get(a).Customer_Name);
//									System.out.println("Customer name: "+customer.CUSTOMERNAME);
									maptoMatch.get(a).Customer_Name=customer.CUSTOMERNAME;
									this.loadTuple(maptoMatch.get(a));
//									this.displayTuple(maptoMatch.get(a));
									check=true;
								}
								else {
									//System.out.println("couldn't match");
									//this.loadTuple(maptoMatch.get(a));
								}
							}
							else {
								System.out.println("is null");
							}
						}
						System.out.println();
						//this.displayTuple(maptoMatch.get(a));
						if(check==false) {
							
							
//							this.loadTuple(maptoMatch.get(a));
//							this.displayTuple(maptoMatch.get(a));
							
						}
						else {
							check=false;
						}
						}
					

					
				
				}
				
			}
            
         //   System.out.println("Queue Remaining size is : "+this.pointersQueue.size());
            if(this.pointersQueue.size()>0)
            {
            	this.removeQueueHead();
            }
        }
			
			
			
		}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		NRD nrt=new NRD();
		nrt.MeshJoin();
	}

}
