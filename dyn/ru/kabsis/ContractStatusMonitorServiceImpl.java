package ru.kabsis;

import java.util.Date;
import java.util.Calendar;
import java.util.List;

import bitel.billing.common.TimeUtils;
import bitel.billing.server.admin.bgsecure.bean.UserGroupManager;

import javax.jws.WebService;
 
import ru.bitel.bgbilling.common.BGException;
import ru.bitel.bgbilling.common.BGMessageException;
import ru.bitel.bgbilling.kernel.contract.status.common.service.ContractStatusMonitorService;
import ru.bitel.common.Utils;
 
@WebService(endpointInterface = "ru.bitel.bgbilling.kernel.contract.status.common.service.ContractStatusMonitorService")
public class ContractStatusMonitorServiceImpl
	extends ru.bitel.bgbilling.kernel.contract.status.server.service.ContractStatusMonitorServiceImpl
	implements ContractStatusMonitorService
{
	//ID группы администраторов
	private final int ADMIN_GROUP = 2;
	
	@Override
	public void changeContractStatus( int[] cids, int statusId, Date dateFrom, Date dateTo, String comment )
	    throws BGException
	{
		UserGroupManager userGroupManager = new UserGroupManager(getConnection());
		List<Integer> groups = userGroupManager.getUserGroups(userId);
		Calendar calendar = Calendar.getInstance();
		Date today = calendar.getTime();
		calendar.add(Calendar.DAY_OF_YEAR, 1);
		Date tomorrow = calendar.getTime();
		
		if(!groups.contains(ADMIN_GROUP))
		{
			if(TimeUtils.dateBefore(dateFrom, tomorrow) & statusId != 0) // если дата начала действия статуса ранее завтрашнегои статус не 0:Активен то выдаем сообщение
			{
				throw new BGMessageException( "Дата смены статуса должны быть позже текущей" );
			}
			if(TimeUtils.dateBefore(dateFrom, today) & statusId == 0) // если дата начала действия статуса ранее сегодняшнего и статус 0:Активен то выдаем сообщение
			{
				throw new BGMessageException( "Дата активации договора должна быть не раньше сегодняшней" );
			}
			if(TimeUtils.dateBefore(dateTo, today)) // если дата окончания действия статуса ранее сегодняшней то выдаем сообщение
			{
				throw new BGMessageException( "Дата закрытия статуса не может быть ранее сегодняшней" );
			}
			if( Utils.isBlankString( comment ) ) // Если поле комментарий пустое то выдаем сообщение
			{
				throw new BGMessageException( "Введите комментарий");
			}
		}
		super.changeContractStatus( cids, statusId, dateFrom, dateTo, comment );
	}
}
