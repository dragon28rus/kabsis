package ru.kabsis;

import java.util.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import javax.jws.WebService;

import bitel.billing.common.TimeUtils;
import bitel.billing.server.admin.bgsecure.bean.UserGroupManager;

import ru.bitel.bgbilling.common.BGException;
import ru.bitel.bgbilling.common.BGMessageException;
import ru.bitel.bgbilling.kernel.contract.status.common.service.ContractStatusMonitorService;
import ru.bitel.common.Utils;

import ru.bitel.bgbilling.kernel.contract.api.common.bean.ContractTariff;
import ru.bitel.bgbilling.kernel.contract.api.server.bean.ContractTariffDao;



@WebService(endpointInterface = "ru.bitel.bgbilling.kernel.contract.status.common.service.ContractStatusMonitorService")
public class ContractStatusMonitorServiceImpl
extends ru.bitel.bgbilling.kernel.contract.status.server.service.ContractStatusMonitorServiceImpl
implements ContractStatusMonitorService {
  private final int ADMIN_GROUP = 2;  //ID группы администраторов
  
//#0:Активен;1:На отключение КТВ;2:Отключен;3:Закрыт;4:Приостановлен;5:В подключении;6:Активен КТВ;7:Активен ЦКТВ;8:На отключение ЦКТВ  
  
  private final Integer[] TV_TARIFF_ID = {39, 65};  //ID тарифов Кабельное ТВ
  private final Integer[] TV_TARIFF_STATUSES = {1, 2, 5, 6};  // разрешенные статусы для этого тарифа
  
  private final Integer[] CKTV_TARIFF_ID = {42, 73};  //ID тарифов Цифровое ТВ
  private final Integer[] CKTV_TARIFF_STATUSES = {2, 5, 7, 8};  // разрешенные статусы для этого тарифа
  
  private final Integer[] VIP_TARIFF_ID = {40};  //ID тарифа ВИП
  private final Integer[] INET_TARIFF_STATUSES = {0, 2, 3, 4, 5};  // разрешенные статусы для интернет тарифов
  
  private final Integer[] ACTIVE_STATUSES = {2, 3, 4, 8};  // активные статусы договоров

  @Override
  public void changeContractStatus(int[] cids, int statusId, Date dateFrom, Date dateTo, String comment)
  throws BGException {
    UserGroupManager userGroupManager = new UserGroupManager(getConnection());
    List < Integer > groups = userGroupManager.getUserGroups(userId);
    Calendar calendar = Calendar.getInstance();
    Date today = calendar.getTime();
    calendar.add(Calendar.DAY_OF_YEAR, 1);
    Date tomorrow = calendar.getTime();

    if (!groups.contains(ADMIN_GROUP)) {
      if (TimeUtils.dateBefore(dateFrom, tomorrow) & Arrays.asList(ACTIVE_STATUSES).contains(statusId)) // если дата начала действия статуса ранее завтрашнегои статус не 0:Активен то выдаем сообщение
      {
        throw new BGMessageException("Дата смены статуса должна быть позже текущей");
      }
      if (TimeUtils.dateBefore(dateFrom, today) & !Arrays.asList(ACTIVE_STATUSES).contains(statusId)) // если дата начала действия статуса ранее сегодняшнего и статус 0:Активен то выдаем сообщение
      {
        throw new BGMessageException("Дата активации договора должна быть не раньше сегодняшней");
      }
      if (TimeUtils.dateBefore(dateTo, today)) // если дата окончания действия статуса ранее сегодняшней то выдаем сообщение
      {
        throw new BGMessageException("Дата закрытия статуса не может быть ранее сегодняшней");
      }
      if (Utils.isBlankString(comment)) // Если поле комментарий пустое то выдаем сообщение
      {
        throw new BGMessageException("Введите комментарий");
      }
    }
    
    // проверка тарифов на договоре
    ContractTariffDao ctd = new ContractTariffDao(getConnection());
    // проверяем для всех cid, чтобы работало и для групповых операций итд, не только для одного договора
    for(int cid : cids)
    {
    	// первый попавшийся активный тариф на договоре
    	ContractTariff ct = ctd.getFirst(cid, new Date());
    	if(ct != null)
    	{
    		// если тариф из таких-то
    		if(Arrays.asList(TV_TARIFF_ID).contains(ct.getTariffPlanId()))
    		{
    			// и если статус НЕ содержится в разрешенных, то сообщение
    			if(!Arrays.asList(TV_TARIFF_STATUSES).contains(statusId))
    			{
    				throw new BGMessageException("Из тарифа ТВ нельзя ставить статус "+statusId);
    			}
    		}
    		if(Arrays.asList(CKTV_TARIFF_ID).contains(ct.getTariffPlanId()))
    		{
    			if(!Arrays.asList(CKTV_TARIFF_STATUSES).contains(statusId))
    			{
    				throw new BGMessageException("Из тарифа Цифровое ТВ нельзя ставить статус "+statusId);
    			}
    		}
			if((!Arrays.asList(VIP_TARIFF_ID).contains(ct.getTariffPlanId())) & (!Arrays.asList(TV_TARIFF_ID).contains(ct.getTariffPlanId())) & (!Arrays.asList(CKTV_TARIFF_ID).contains(ct.getTariffPlanId())))
    		{
    			if(!Arrays.asList(INET_TARIFF_STATUSES).contains(statusId))
    			{
    				throw new BGMessageException("Из итнертнет тарифа нельзя ставить статус "+statusId);
    			}
    		}
    	}
    }
    
    super.changeContractStatus(cids, statusId, dateFrom, dateTo, comment);
  }
}
