
public class Roles {
	public boolean isClient;
	public boolean isServer;
	public boolean isProxy;
	
	public Roles(){
		this.isClient = false;
		this.isProxy = false;
		this.isServer = false;
	}
	public void listRoles(){
		System.out.println("Roles: isClient: "+ this.isClient +", isProxy: "+ this.isProxy+", isServer: " +  this.isServer);
	}
}