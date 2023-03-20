import java.util.Date;

public class TransactionData {
    public Integer Transaction_ID;
    public String Product_ID;
    public String Customer_ID; //remove later maybe as not needed for query?
    public String Customer_Name;
    public String Store_ID;
    public String Store_Name;
    public Date T_Date;
    public Integer Quantity;
    public String Product_Name=null;
    public String Supplier_ID;
    public String Supplier_Name;
    public Double Total_Sale;
    
    public void printTD()
    {
    	System.out.println("TransactionID: "+this.Transaction_ID);
    	System.out.println("Product_ID: "+this.Product_ID);
    	System.out.println("Customer_ID: "+this.Customer_ID);
    	System.out.println("Store_ID: "+this.Store_ID);
    	System.out.println("Store_Name: "+this.Store_Name);
    	System.out.println("T_Date: "+this.T_Date);
    	System.out.println("Quantity: "+this.Quantity);
    	System.out.println();
    	System.out.println();


    }
}
