package ru.kabsis.modules.cashcheck;

import java.sql.Connection;
import java.util.Set;

import ru.bitel.bgbilling.common.BGException;
import ru.bitel.bgbilling.kernel.module.common.bean.User;
import ru.bitel.bgbilling.kernel.module.server.bean.UserManager;
import ru.bitel.bgbilling.plugins.cashcheck.common.Payment;
import ru.bitel.bgbilling.plugins.cashcheck.common.Printer;
import ru.bitel.bgbilling.plugins.cashcheck.server.CheckBuilder;
import ru.bitel.bgbilling.plugins.cashcheck.server.bean.Check;
import ru.bitel.bgbilling.server.util.Setup;
import bitel.billing.server.contract.bean.ContractParameterManager;
import bitel.billing.server.contract.bean.ContractAddressParamValue;

public class SimpleCheck
	implements CheckBuilder
{
	private Connection con;
	
	@Override
	public void init( Setup setup, Connection con, Connection conSlave )
		throws BGException
	{
		this.con = con;
	}

	@Override
	public void addPayment( Payment payment, Check check, Printer printer )
		throws BGException
	{
		check.addString( "----------------------------------------------------------" );
		check.addPayment( payment.getSumma(), "Принята сумма: " +  payment.getSumma() + "р.", 0 );	
		check.addString( "----------------------------------------------------------" );
		check.addString( "Оплата по договору: " + payment.getContractTitle());
//Параметр договора Ф.И.О.
		int cid = payment.getContractID();
		int PARAM_ID_FIO = 2;
		ContractParameterManager contractParameterManager = new ContractParameterManager(con);
			String paramValFio = contractParameterManager.getStringParam( cid, PARAM_ID_FIO );
			if (paramValFio != null)
			{
				check.addString( "Платёж от: " + paramValFio);
			}
//Параметр договора (адрес)
		int PARAM_ID = 20;
			ContractAddressParamValue ap = contractParameterManager.getAddressParam(cid, PARAM_ID);
			String adress = ap.getAddress();
			String[] adressParamValue = adress.split(",");
			int razmer = adressParamValue.length;
         check.addString( "Адрес подключения: ");
		if(adressParamValue[1].equalsIgnoreCase(" Благовещенск"))
		{
			if (adressParamValue.length >= 6)
			{
			check.addString( adressParamValue[3] + "," + adressParamValue[4] + "," + adressParamValue[5]);
			}
			else
			{
				check.addString( adressParamValue[3] + "," + adressParamValue[4]);
			}
		}
		else if (adressParamValue[1].equals(" с. Плодопитомник"))
		{
			if (adressParamValue.length >= 5)
			{
				check.addString( adressParamValue[1] + ","  + adressParamValue[2] + ","  + adressParamValue[3] + ","  + adressParamValue[4]);
			}
			else
			{
				check.addString( adressParamValue[1] + "," + adressParamValue[2] + "," + adressParamValue[3]);
			}
		}
		else if (adressParamValue[1].equals(" с. Чигири"))
		{
			if (adressParamValue.length >= 5)
			{
				check.addString( adressParamValue[1] + "," + adressParamValue[2] + "," + adressParamValue[3] + "," + adressParamValue[4]);
			}
			else
			{
				check.addString( adressParamValue[1] + "," + adressParamValue[2] + "," + adressParamValue[3]);
			}
		}
		check.addString( "----------------------------------------------------------" );
// Вывод комментария к платежу в случае его наличия
//		if(payment.getComment().isEmpty()) 
//		{
//			check.addString( payment.getComment() );
//		}
//		else
//		{
//			check.addString( " " );
//			check.addString( payment.getComment() );
//		}
        //ФИО кассира (пользователя биллинга)
		UserManager um = new UserManager( con );   
		User user = um.get( payment.getUserId() );
		check.addString( "Касир :" + user.getName() );
	}

	public void endCreate( int cid, Check check, Printer printer )
		throws BGException
	{
		// ничего нету, устаревший метод
	}

	@Override
	public void endCreate( Set<Integer> cids, Check check, Printer printer )
		throws BGException
	{
		check.addString( "Удачного дня!" );
	}
}

