package bdt;

import java.io.Serializable;

import lombok.Data;


@Data
public class StudentExpense implements Serializable{
	private static final long serialVersionUID = 1L;
	private String id;
	private String gender;
	private String age;
	private String studyYear;
	private String living;
	private String scholarship;
	private String partTimeJob;
	private String transporting;
	private String monthlySubsciption;
	private String monthlyExpnses;
}
