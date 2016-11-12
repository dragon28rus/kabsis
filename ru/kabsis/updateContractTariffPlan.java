package ru.kabsis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
 
import bitel.billing.common.TimeUtils;
import bitel.billing.server.admin.bgsecure.bean.UserGroupManager;
 
 
import ru.bitel.bgbilling.common.BGException;
import ru.bitel.bgbilling.common.BGMessageException;

public class updateContractTariffPlan
	extends bitel.billing.server.contract.action.ActionUpdateContractTariffPlan
{
		//ID группы администраторов
	private final int ADMIN_GROUP = 2;
 
	@Override
    public void doAction() 
    	throws SQLException, BGException
	{
		UserGroupManager userGroupManager = new UserGroupManager(con);
		List<Integer> groups = userGroupManager.getUserGroups(userID);
		Calendar calendar = Calendar.getInstance();
		Date today = calendar.getTime();
		calendar.add(Calendar.DAY_OF_YEAR, 1);
		Date tomorrow = calendar.getTime();
		String id = getParameter("id", "");
		Date date2 = TimeUtils.convertStringToDate(getParameter( "date2", "" ));
		Date date1 = TimeUtils.convertStringToDate(getParameter( "date1", "" ));
 
		if(!groups.contains(ADMIN_GROUP))
		{
			if(!id.equals("new") & TimeUtils.dateBefore(date2, today))
			{
				throw new BGMessageException( "Дата закрытия тарифного плана не может быть ранее текущей" );
			}
			if(id.equals("new") & TimeUtils.dateBefore(date1, tomorrow))
			{
				String query = 	" SELECT COUNT(id) FROM contract_tariff WHERE cid=? ";
				java.sql.PreparedStatement ps = con.prepareStatement( query );
				ps.setInt(1, cid);
				ResultSet rs = ps.executeQuery();
				while ( rs.next() )
				{
					int count = rs.getInt(1);
					if(count > 0 )
					{
						throw new BGMessageException( "Тарифный план должен устанавливаться датой следующей за текущей" );
					}
				}
				rs.close();
				ps.close();
			}
		}
		// иначе вызываем родительский метод
		super.doAction();
	}
}