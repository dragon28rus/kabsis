package ru.kabsis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.GregorianCalendar;

import bitel.billing.common.TimeUtils;
import ru.bitel.bgbilling.kernel.script.server.dev.GlobalScriptBase;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.Utils;
import ru.bitel.common.sql.ConnectionSet;
import bitel.billing.server.contract.bean.Contract;
import bitel.billing.server.contract.bean.ContractManager;

public class GlobalContractMoveGroup
	extends GlobalScriptBase
{
    /** группа для обработки */
    private final static int GROUP_TEST = 12;
    private final static long GROUP_TEST_LONG = Utils.enumToMask( String.valueOf( GROUP_TEST ));
    private final static int GROUP_CLOSE = 13;
    private final static long GROUP_CLOSE_LONG = Utils.enumToMask( String.valueOf( GROUP_CLOSE ));
    
    private final static int STATUS_CLOSE =3; 
    private final static int DAY_PERIOD1 =10;
    private final static int DAY_PERIOD2 =90;
    
    private ContractManager cm ;

	@Override
	public void execute( Setup setup, ConnectionSet connectionSet )
		throws Exception
	{
	    Connection con = connectionSet.getConnection();
        cm = new ContractManager( con );
                
        PreparedStatement ps = con.prepareStatement( "SELECT ct.id, st.date1 "
        + " FROM contract   AS ct"
        + " JOIN contract_status AS st ON ct.status=? AND ct.date2 IS NULL AND st.status =? AND ct.id = st.cid "
        + " AND (st.date1 IS NULL OR st.date1 <= ?) AND (st.date2 IS NULL OR st.date2 >= ?) "
        + " WHERE gr & ? = 0" );
        
        Calendar now =  new GregorianCalendar();
        int index =1;
        ps.setInt( index++, STATUS_CLOSE );
        ps.setInt( index++, STATUS_CLOSE );
        ps.setDate( index++, TimeUtils.convertCalendarToSqlDate( now ) );
        ps.setDate( index++, TimeUtils.convertCalendarToSqlDate( now ) );
        ps.setLong( index++, GROUP_CLOSE_LONG );
        ResultSet rs = ps.executeQuery();
        int count = 0;
        int countAll = 0;
        while ( rs.next() )
        {
            countAll++;
            int cid = rs.getInt( 1 );
            Calendar dateFrom = TimeUtils.convertSqlDateToCalendar( rs.getDate( 2 ) );
            Contract contract = cm.getContractById( cid );
            StringBuffer  result = new  StringBuffer();
            result.append( contract.getTitle()+"["+contract.getId()+"]=>" );
            if ( (contract.getGroups() & GROUP_TEST_LONG) == GROUP_TEST_LONG )
            {
                if ( TimeUtils.daysDelta( dateFrom, now ) <= DAY_PERIOD1 )
                {
                    result.append( "Тестовая группа,"+TimeUtils.formatDate( dateFrom)+" меньше " + DAY_PERIOD1 + " дней=>ПРОПУЩЕН" );
                    print(result.toString());
                    continue;
                }
            }
            else if ( TimeUtils.daysDelta( dateFrom, now ) <= DAY_PERIOD2 )
            {
                result.append( TimeUtils.formatDate(dateFrom)+"меньше " + DAY_PERIOD2 + " дней => ПРОПУЩЕН" );
                print(result.toString());
                continue;
            }
            result.append( TimeUtils.formatDate(dateFrom)+" Устанавливаем группу" );
            print(result.toString());
            
            //чистим группу
//            if(cid!=79)
//            {
//                result.append( "тест => ПРОПУЩЕН" );
//                continue;
//            }
            contract.setGroups( contract.getGroups()|GROUP_CLOSE_LONG );
			cm.updateContract( contract );
			cm.closeContract( cid );
           
            count++;
        }
        print( "Выбрано "+countAll+" => обработано " + count + " договоров" );
        cm.recycle();
        ps.close();
	}
}
