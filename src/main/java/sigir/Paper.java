package sigir;

public class Paper {
	
	public String acmID;
	public String title;
	public String abstractText;
	public String conferenceID;
	
	public Paper(int id, String title, String abstractText, String conferenceId)
	{
		this.acmID = String.valueOf(id);
		this.title = title;
		this.abstractText = abstractText;
		this.conferenceID = conferenceId;
				
	}

}
