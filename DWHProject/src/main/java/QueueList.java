
public class QueueList {
	public String ProductID;
	public String CustomerID;
	
	public QueueList(String prod,String cust) {
		this.ProductID=prod;
		this.CustomerID=cust;
	}
	public void printQueueList() {
		System.out.println("ProductId: "+this.ProductID+ " "+ "CustomerID: "+this.CustomerID);
	}
	
}
