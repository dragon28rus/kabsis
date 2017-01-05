package ru.kabsis;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import bitel.billing.common.TimeUtils;
import bitel.billing.server.contract.bean.Contract;
import bitel.billing.server.contract.bean.ContractManager;
import ru.bitel.bgbilling.common.BGMessageException;
import ru.bitel.bgbilling.kernel.convert.ConvertUtil;
import ru.bitel.bgbilling.kernel.module.common.bean.User;
import ru.bitel.bgbilling.kernel.script.server.dev.GlobalScriptBase;
import ru.bitel.bgbilling.modules.cerbercrypt.common.bean.Card;
import ru.bitel.bgbilling.modules.cerbercrypt.common.bean.CardPacket;
import ru.bitel.bgbilling.modules.cerbercrypt.common.bean.UserCard;
import ru.bitel.bgbilling.modules.cerbercrypt.server.bean.CardManager;
import ru.bitel.bgbilling.modules.cerbercrypt.server.bean.CardPacketManager;
import ru.bitel.bgbilling.modules.cerbercrypt.server.bean.UserCardManager;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.Utils;
import ru.bitel.common.sql.ConnectionSet;
import bitel.billing.server.contract.bean.ContractStatus;
import bitel.billing.server.contract.bean.ContractStatusManager;

/**
 * Конвертер договоров из csv, HD#5602
 * @author dimon
 */
public class ConvertContracts
	extends GlobalScriptBase
{
	//--- параметры, которые надо поменять --------------------------------------------------------------

	private final static String CSV_abonent =           "/home/bgadmin/1cToBGBilling/27.12/abonent.csv";
	private final static String CSV_abonent_comment =   "/home/bgadmin/1cToBGBilling/27.12/abonent_comment.csv";
	private final static String CSV_ostatki =           "/home/bgadmin/1cToBGBilling/27.12/ostatki.csv";
	private final static String CSV_payment =           "/home/bgadmin/1cToBGBilling/27.12/payment.csv";
	private final static String CSV_digitcart_abonent = "/home/bgadmin/1cToBGBilling/27.12/digitcart_abonent.csv";
	private final static String CSV_tarif_abonent =     "/home/bgadmin/1cToBGBilling/27.12/tarif_abonent.csv";
	
	/** Группа, КОТОРАЯ ЧИСТИТСЯ полностью перед конвертированием, и в которую заново попадают все договоры, если -1 то не удаляется ничего и не ставится */
	private static int IMPORT_GROUP_ID = 11;
	
	/** ид шаблона договора по умолчанию */
	private static int TEMPLATE_DEFAULT = 4;
	/** ид шаблона для КодРодителя=3062 */
	private static int TEMPLATE_3062 = 5;
	
	/** группа которая вешается КодРодителя=2887 */
	private static int GROUP_2887 = 3;
	/** ид тарифа которая вешается КодРодителя=2887 */
	private static int TARIF_2887 = 40;
	
	/** группа которая вешается если никакой другой не было добавлено */
	private static int GROUP_DEFAULT = 7;
	/** ид тарифа которая вешается если никакой другой тариф не было добавлено */
	private static int TARIF_DEFAULT = 39;
	
	/** параметр для "Ф.И.О." - тип текстовый */
	private static int PID_FIO = 2;
	/** параметр для "Улица" "Дом" "Квартира" - тип адрес */
	private static int PID_ADDRESS = 20;
	/** параметр для "Телефон" - тип телефон */
	private static int PID_PHONE = 16;
	/** параметр для "Документ"+"Реквизиты и кем выдан документ" - тип текстовый */
	private static int PID_DOC = 11;
	/** параметр для комментария из abonent_comment - тип текстовый */
	private static int PID_COMMENT = 29;
	
	/** группа для "цифрового договора" */
	private static int GROUP_DIGIT = 8;
	/** ид тарифа для "цифрового договора" */
	private static int TARIF_DIGIT = 42;
	/** mid модуля для "цифрового договора" он же код модуля церберкрипт */
	private static int MODULE_DIGIT = 9;
	
	/** тип для платежа "ввод остатков "*/
	private static int PAYMENT_TYPE = 7;
	/** тип для расхода "ввод остатков "*/
	private static int CHARGE_TYPE = 22;
	
	/** соответствие кодов пакетов (ид которые по ctrl+i, а не текстовых!) в биллинга названиям пакетов в csv */
	// если стоит Социальный то на карту добавляем пакет "3", если Базовый то на карту добавляем пакеты "3 и 4"
	private static Map<String, int[]> DIGIT_PACKETS = new HashMap<String, int[]>()
	{{
		put("Социальный", new int[]{3});
		put("Базовый", new int[]{3, 4});
	}};
	
	/** ид страны для адресов */
	private static int ADDRESS_COUNTRY_ID = 1;
	
	/** комментарий добавляемого на договор тарифа */
	private final static String TARIFFS_COMMENT = "импортировано";
	/** комментарий добавляемой на договор карты */
	private final static String UC_COMMENT = "импортировано";
	/** комментарий добавляемого на договор статуса */
	private final static String STATUS_COMMENT = "импортировано";
	/** шаблон строкового заголовка который получается из целого номера договора */
	private final static String CONTRACT_TITLE_PTRN = "%d";
	
	//-- ниже менять не надо ----------------------------------------------------------------------------
	
	/*
	abonent.csv
	Создаем договр с шаблоном ID 4
	1. Гр/Элем - если 1 (группа) то не переносим
	2. КодРодителя - если 2887 то добавляем группу ID 3 и тариф ID 40,
	если 3062 то создаем договр с шаблоном ID 5,
	остальные коды не учитываем
	3. Код абонента - № создаваемого договора
	4. Ф.И.О. - Заносим в параметр № 2(тип поля текстовый) создаваемого договора
	5. Улица - Улица подключения параметр №20(тип поля адрес) в биллинге (все адреса в биллинге уже заведены)
	6. Дом - Дом подключения параметр №20 в биллинге (не найденные адреса можно завети как "пользовательское значение адреса" а потом ручками поправим)
	7. Квартира - Квартира подключения параметр №20 в биллинге
	8. Телефон - В 1с два значения для телефона (зачем то) телефон просто и телефон для смс, берем первый номер, заносим в параметр №16(тип поля телефон) из биллинга
	9. Документ - Документ на основании которого заключен договор (тут полный пи...) перед основным переносом постораюсь поправить но переносим как есть в параметр ID 11(тип поля текстовый) текстовым видом
	10. Реквизиты и кем выдан документ - добавляем к пункту 9
	11. Дата заключения договора - от этого числа создаем договор
	12. Наличие аналогового договора (Если есть то 1) - аналоговый договор есть у всех и данный параметр можно пропустить
	13. Наличие цифрового договора (Если есть то 1) - если 1 то добавляем группу ID 8, обавляем тариф ID 42 и добавляем модуль с кодом 9 (CerberCrypt)
	14. Дата расторжения договора если есть то закрываем договор той датой
	Группу "Импортированные" создал, ID 11.
	*/
	/*
	1) вы убираете из шаблона тариф и группу ему соответствующую (видимо, ID 39 и ID 7)
	2) если код родителя 2887 то (это вип): добавляем группу ID 3 и тариф ID 40
	3) далее, если цифровой договор (в 13й колонке стоит 1) то добавляем группу ID 8 и модуль MID 9, и ЕСЛИ тарифа не было добавлено ("тариф вип" пока только пытались), то и тариф  ID 42
	4) далее, если тарифа не было добавлено никакого (ни вип, ни цифрового), то добавляем тариф ID 39 и группу ID 7
	*/
	/*
	Если договор уже был то тарифы вообще не трогаем, группы добавляем, модуль для цифрового тв добавляем, карты добавляем
	*/
	// 0;2887;16154;Боженко Сергей Федорович     ;Рябиновая   ;  ;0;89145385105 89145385105;паспорт;1001 232141 УВД г.Благовещенска 17.12.2001г;04.12.15;1;1;;
	// 0;2887;13235;Бывший офис Кабельные системы;Институтская;11;0;89622847635 89622847635;       ;                                           ;13.11.13;1; ;;
	private final static String[] CSV_FIELDS_abonent = {"gr","parent","contract","fio","city","street","house","flat","tel","doc","doc_rekv","date1","ct_analog","ct_digit","date2"};

	/*
	abonent_comment.csv
	1. КодАбонента - номер договора которому дописываем комментарий
	2. Комментарий - переносим в параметр ID 29 (текстовый)
	 */
	// 7885;АГРОЦЕНТР НАШ КОМПЬЮТЕРЩИК;
	private final static String[] CSV_FIELDS_abonent_comment = {"contract", "comment"};

	/*
	ostatki.csv
	В 1й строке указано на какое число остатки (той датой и заносм)
	1. Ф.И.О. - пропускаем
	2. КодАбонента - номер договора
	3. Сумма
	4. ЧейДолг - если стоит "нашДолг" то сумму заносим в приход, тип платежа ID 7 (ввод остатков). если стоит "долгКлиента" то сумму заносим в расход, тип расхода ID 22 (ввод остатков)
	*/
	private final static String[] CSV_FIELDS_ostatki = {"fio", "contract", "sum", "flagpaycha"};
	private final static String CSV_FIELDS_ostatki_payment = "нашДолг";
	private final static String CSV_FIELDS_ostatki_charge = "долгКлиента";
	private final static Pattern CSV_FIELDS_ostatki_date_regexp_first_line = Pattern.compile(".*(\\d\\d)\\.(\\d\\d)\\.(\\d\\d).*"); // "Остатки на дату 31.12.15"
	
	/*
	payment.csv
	1. Код абонента - номер договора
	2. Вид документа - выносим в комментарий
	3. Дата документа - этой датой делаем документ
	4. Номер документа - выносим в комментарий
	5. Тип операции - выносим в комментарий
	6. Сумма - сумма операции
	7. Статус - если "уменДолга" то сумму заносим в приход, тип платежа ID 7 (ввод остатков). если стоит "увелДолга" то сумму заносим в расход, тип расхода ID 22 (ввод остатков)
	комментарий операции следующего формата "вид документа - номер док - тип операции"
	*/
	// 15267;ПодключениеКлиента;01.01.16;87159;_Подключение;480;увелДолга;
	private final static String[] CSV_FIELDS_payment = {"contract", "comment1", "date", "comment2", "comment3", "sum", "flagpaycha"};
	private final static String CSV_FIELDS_payment_payment = "уменДолга";
	private final static String CSV_FIELDS_payment_charge = "увелДолга";
	private final static String CSV_FIELDS_payment_dateformat = "dd.MM.yy";
	
	/*
	digitcart_abonent.csv
	1. Код абонента - номер договора
	2. НомерКарты - полный номер карты, нам необходимо откинуть точку в конце строи и последнию цифру
	3. НазваниеПакета - действующий пакет на карте, если стоит Социальный то на карту добавляем пакет "2", если Базовый то на карту добавляем пакеты "1 и 2"
	4. Состояние - если "расторгнут" то в комментарии к карте добавляем что она расторгнута и не добавляем пакеты
	*/
	// КодАбонента; НомерКарты; НазваниеПакета; Состояние (Действует/Приостановлен/Расторгнут)
	// 15732;7214204000036777.;Базовый;Действует;
	private final static String[] CSV_FIELDS_digitcart_abonent = {"contract", "card", "packet", "status"};
	private final static String CSV_FIELDS_digitcart_abonent_active = "Действует";
	private final static String CSV_FIELDS_digitcart_abonent_suspend = "Приостановлен";
	private final static String CSV_FIELDS_digitcart_abonent_stop = "Расторгнут";
	
	/*
	tarif_abonent.csv
	код абонента - номер договора
	тип договора - где стоит цифровой пропускаем строку
	дата заключения - пропускаем столбец т.к. берем эту дату с другого файла
	дата подключения - ...см.ниже...
	стоимость - пропускаем столбец
	статус - текущий статус договора  - если Действует то в bg ставим 0:Активен, если Приоставновлен или Расторгнут то в bg ставим 2:Отключен
	код тарифа - пропускаем столбец
	Брать дату подключения и проверять если эта дата раньше 1го часла текущего месяца то договор активировать
	(установить статус договора 0:Активен) 1м часлом текущего месяца, если же дата позже 1го числа текущего месяца
	то ставить статус активен тем числом что указан в файле.
	*/
	//Код абонента; ТипДоговора; Дата заключения; Дата подключения; Стоимость; Статус; КодТарифа;
	//7671;Аналоговый;24.11.08;26.11.08;0;Действует;1050;
	private final static String[] CSV_FIELDS_tarif_abonent = {"contract", "type", "_date", "date", "_cost", "status", "_code"};
	private final static String CSV_FIELDS_tarif_abonent_analog = "Аналоговый";
	private final static String CSV_FIELDS_tarif_abonent_digit = "Цифровой";
	private final static String CSV_FIELDS_tarif_abonent_stat_active = "Действует";
	private final static String CSV_FIELDS_tarif_abonent_stat_noactive1 = "Приостановлен";
	private final static String CSV_FIELDS_tarif_abonent_stat_noactive2 = "Расторгнут";
	private final static int CSV_FIELDS_tarif_abonent_setstatus_active_analog = 6;
	private final static int CSV_FIELDS_tarif_abonent_setstatus_active_digit = 7;
	private final static int CSV_FIELDS_tarif_abonent_setstatus_noactive = 2;

	/** сюда разбираются абоненты */
	private List<BgAbonent> contractList = new ArrayList<BgAbonent>();
	/** сюда разбираются каменты договора договор=>камент */
	private Map<Integer, String> contractCommentMap = new HashMap<Integer, String>();
	/** сюда разбираются платежи/расходы (и остатки и платежи) договор=>[список платежей/расходов] */
	private Map<Integer, List<BgPayment>> contractPaymentMap = new HashMap<Integer, List<BgPayment>>();
	private Map<Integer, List<BgPayment>> contractChargeMap = new HashMap<Integer, List<BgPayment>>();
	/** сюда разбирается подписка cerbercrypt договор=>BgCardPacket */
	private Map<Integer, List<BgCardPacket>> contractTvSubscription = new HashMap<Integer, List<BgCardPacket>>();
	/** сюда разбираются установки статусов */
	private Map<Integer, BgSetStatus> contractSetStatusMap = new HashMap<Integer, BgSetStatus>();
	
	@Override
	public void execute(Setup setup, ConnectionSet connectionSet)
		throws Exception
	{
		long start = System.currentTimeMillis();
		
		Connection con = connectionSet.getConnection();
		
		try(ConvertUtil convertUtil = new ConvertUtil(setup, con))
		{		
			// удаление договоров из группы
			if(IMPORT_GROUP_ID>=0)
			{
				System.out.println( "deleting contracts..." );
				int count = convertUtil.removeContracts( IMPORT_GROUP_ID );
				System.out.println( " " + count + " contracts was deleted");
				connectionSet.commit();
			}
			convertUtil.removeCerberCryptGarbage( MODULE_DIGIT );
			connectionSet.commit();
	
			// загрузка абонентов
			loadAbonent();
	
			// проверка адресов на заведённость в системе итд
			checkAddress(convertUtil);
	
			// загрузка комментов
			loadAbonentComment();
	
			// загрузка остатков
			loadOstatki();
			
			// загрузка платежей
			loadPayment();
			
			// загрузка карт cerbercypt
			loadDigitcartAbonent();
			
			// загрузка tarif_abonent
			loadTarifAbonent();
			
			// понеслось
			createContracts(convertUtil, con);
		}
		finally
		{
			System.out.println("execute time: " + (System.currentTimeMillis() - start) + "ms");	
		}
	}
	
	/** Создаём договоры и всё остальное из разобранных данных */
	private void createContracts(ConvertUtil convertUtil, Connection con)
		throws Exception
	{
		System.out.println( "create contracts..." );
		int numline = 0;
		for(BgAbonent abonent : contractList)	
		{
			++numline;
//			System.out.println( " ["+numline+"/"+contractList.size()+"] abonent "+abonent.contract+" ("+abonent.fio+")..." );
			// сначала ищем по адресу существующий договор
			// то есть если нашли по адресу, то делаем ТОЛЬКО добавление модуля+группы церберкрипта,
			// ну и в модуль загружаем сооьтветствующую информацию. основной договор вообще не трогаем.
			Contract contract = convertUtil.findContract(PID_ADDRESS, abonent.houseId, abonent.flat);
			boolean findonaddress = false; // true - если договор уже существовал и нашёлся по адресу
			boolean wasaddtariff = false; // true - если был добавлен тариф (нам надо чтобы в итоге не более одного тарифа было, потому последовательно пытаемся добавить vip->цифровой->default)
			if(contract!=null) // если нашёлся договор - ничего не делаем, перейдём к добавлению сущностей церберкрипта
			{
				System.out.println( "  contract found for address: "+contract );
				findonaddress = true;
			}
			else // если НЕ нашёлся договор - делаем все действия, а потом уже добавление сущностей церберкрипта
			{
				// шаблон
				int pattern_id = TEMPLATE_DEFAULT;
				if(abonent.parent == 3062)
				{
					pattern_id = TEMPLATE_3062;
				}
				// тайтл: 3. Код абонента - № создаваемого договора
				// 11. Дата заключения договора - от этого числа создаем договор
				String title = String.format(CONTRACT_TITLE_PTRN, abonent.contract);
				//
				try
				{
					contract = convertUtil.createContract(pattern_id, title, abonent.fio, abonent.date1);
				}
				catch(BGMessageException e)
				{
					System.out.println("  [!] contract "+abonent.contract+": exception create contract: "+e.getMessage());
				}
				//
				if( contract == null )
				{
					System.out.println("  [!] contract "+abonent.contract+": error create contract");
					continue;
				}
	//			System.out.println( "  contract create from pattern "+pattern_id+": "+contract );
				// группа обязательная для всех
				if(IMPORT_GROUP_ID>=0)
				{
					convertUtil.addContractGroup(contract.getId(), IMPORT_GROUP_ID);
				}
				// 2. КодРодителя - если 2887 то добавляем группу ID 3 и тариф ID 40,
				if(abonent.parent == 2887)
				{
	//				System.out.println( "  add group/tariff of parent 2887...");
					convertUtil.addContractGroup(contract.getId(), GROUP_2887);
					convertUtil.addContractTariff(contract.getId(), TARIF_2887, abonent.date1, TARIFFS_COMMENT);
					wasaddtariff = true;
				}
				// 4. Ф.И.О. - Заносим в параметр № 2(тип поля текстовый) создаваемого договора
				if(abonent.fio != null)
				{
	//				System.out.println( "  add fio parameter...");
					convertUtil.addContractParameter(contract.getId(), PID_FIO, abonent.fio);
				}
				// Адрес
				//5. Улица - Улица подключения параметр №20(тип поля адрес) в биллинге (все адреса в биллинге уже заведены)
				//6. Дом - Дом подключения параметр №20 в биллинге (не найденные адреса можно завети как "пользовательское значение адреса" а потом ручками поправим)
				//7. Квартира - Квартира подключения параметр №20 в биллинге
				convertUtil.addContractAddressParameter(contract.getId(), PID_ADDRESS, ADDRESS_COUNTRY_ID, abonent.cityId, abonent.streetId, abonent.houseId, abonent.flat, abonent.street, abonent.house);
				//9. Документ - Документ на основании которого заключен договор (тут полный пи...) перед основным переносом постораюсь поправить но переносим как есть в параметр ID 11(тип поля текстовый) текстовым видом
				//10. Реквизиты и кем выдан документ - добавляем к пункту 9
				String doc_full = abonent.doc;
				if(abonent.doc_rekv != null)
				{
					if( doc_full == null ) doc_full = "?";
					doc_full += "-" + abonent.doc_rekv;
				}
				if(doc_full != null)
				{
	//				System.out.println( "  add document parameter...");
					convertUtil.addContractParameter(contract.getId(), PID_DOC, doc_full);
				}
				//8. Телефон - В 1с два значения для телефона (зачем то) телефон просто и телефон для смс, берем первый номер, заносим в параметр №16(тип поля телефон) из биллинга
				if(abonent.tel != null)
				{
	//				System.out.println( "  add phone parameter...");
					convertUtil.addContractParameter(contract.getId(), PID_PHONE, abonent.tel);
				}
				
				// комментарий
				String conComment = contractCommentMap.get(abonent.contract);
				if(conComment!=null)
				{
	//				System.out.println( "  add contract comment...");
					convertUtil.addContractParameter(contract.getId(), PID_COMMENT, conComment);
				}
				
				// платежи
				List<BgPayment> payments = contractPaymentMap.get(abonent.contract);
				if(payments!=null && !payments.isEmpty())
				{
					for(BgPayment p : payments)
					{
	//					System.out.println( "  add contract payment ("+p.date+", "+p.sum+")...");
						convertUtil.addContractPayment(contract.getId(), PAYMENT_TYPE, p.date, p.sum, p.comment);
						convertUtil.setBalance(contract.getId(), p.date); // обновляем баланс
					}
				}
				
				// расходы
				List<BgPayment> charges = contractChargeMap.get(abonent.contract);
				if(charges!=null && !charges.isEmpty())
				{
					for(BgPayment c : charges)
					{
	//					System.out.println( "  add contract charge ("+c.date+", "+c.sum+")...");
						convertUtil.addContractCharge(contract.getId(), CHARGE_TYPE, c.date, c.sum, c.comment);
						convertUtil.setBalance(contract.getId(), c.date); // обновляем баланс
					}
				}
				
				// установки статусов
				BgSetStatus setstatus = contractSetStatusMap.get(abonent.contract);
				if(setstatus!=null)
				{
					convertUtil.setStatus(contract.getId(), setstatus.status, setstatus.date, STATUS_COMMENT);
				}
			
			} // конец формирования договора, если не нашёлся по адресу

			// 13. Наличие цифрового договора (Если есть то 1) - если 1 то добавляем группу ID 8, обавляем тариф ID 42 и добавляем модуль с кодом 9 (CerberCrypt)
			// тариф добавляем только если НЕ было добавлено тариф ("вип"?)
			// тариф добавляем только если договор не был найден по адресу (а не создавался нами)
			if(abonent.ct_digit == 1)
			{
//				System.out.println( "  add digit contract...");
				convertUtil.addContractGroup(contract.getId(), GROUP_DIGIT);
				if(!wasaddtariff && !findonaddress)
				{
					convertUtil.addContractTariff(contract.getId(), TARIF_DIGIT, abonent.date1, TARIFFS_COMMENT);
					wasaddtariff = true;
				}
				convertUtil.addContractModule(contract.getId(), MODULE_DIGIT);
			}
			
			// далее, если тарифа не было добавлено никакого (ни вип, ни цифрового), то добавляем тариф ID 39 и группу ID 7
			if(!wasaddtariff)
			{
				// тариф добавляем только если договор не был найден по адресу (а не создавался нами)
				if(!findonaddress)
				{
					convertUtil.addContractTariff(contract.getId(), TARIF_DEFAULT, abonent.date1, TARIFFS_COMMENT);
				}
				convertUtil.addContractGroup(contract.getId(), GROUP_DEFAULT);
			}

			// сущности церберкрипта
			CardManager cardManager = new CardManager(con, MODULE_DIGIT, User.USER_SERVER );
			UserCardManager ucm = new UserCardManager(con, MODULE_DIGIT, User.USER_SERVER);
			CardPacketManager cpm = new CardPacketManager(con, MODULE_DIGIT);
			List<BgCardPacket> tvSubscription = contractTvSubscription.get(abonent.contract);
			if(tvSubscription!=null && !tvSubscription.isEmpty())
			{
				if(abonent.ct_digit == 1)
				{
					for(BgCardPacket sbs : tvSubscription)
					{
//						System.out.println( "  add tv subscription...");
						// получаем карту, чтобы проверить что такая есть (и потом её обновить договор заодно)
						Card card = cardManager.getCard( sbs.cardnumber );
				        if( card == null )
				        {
				        	System.out.println("  [!] contract "+abonent.contract+": not registered card: "+sbs.cardnumber );
				        }
				        if( card != null && card.getDealerId() <= 0 )
				        {
				        	System.out.println("  [!] contract "+abonent.contract+": not dealer card: "+sbs.cardnumber );
				        }
						// карту добавляем
//						System.out.println( "   add card "+sbs.cardnumber+"...");
ContractManager contManager = new ContractManager( con );
int status = contManager.getContractById(contract.getId()).getStatus();
BgSetStatus setstatus = contractSetStatusMap.get(abonent.contract);

						UserCard uc = new UserCard();
						uc.setContractId(contract.getId());
						uc.setNumber(sbs.cardnumber);
						uc.setDate1(abonent.date1);
						uc.setComment(UC_COMMENT);
						ucm.update(uc);
						// пакеты добавляем
						int packetIds[] = DIGIT_PACKETS.get(sbs.packet);
						if(packetIds!=null && packetIds.length>0 && (status == 0 || status == 7 || status == 6))
						{
							for(int packetId : packetIds)
							{
//								System.out.println( "   add packet "+packetId+"...");
								CardPacket cardPacket = new CardPacket();
								cardPacket.setContractId(contract.getId());
								cardPacket.setUsercardId(uc.getId());
								cardPacket.setDateFrom(setstatus.date);
								cardPacket.setPacketId(packetId);
								cpm.updateCardPacket(cardPacket);
							}
						}
						else
						{
							System.out.println("  [!] contract "+abonent.contract+": unknown packet: "+sbs.packet);
						}
						// обновляем договор карты
						if(card != null)
						{
							cardManager.updateCardContract( card, new GregorianCalendar() );
						}
					}
				}
				else
				{
					System.out.println("  [!] contract "+abonent.contract+": has tvSubscription not has ct_digit");
				}
			}
			ucm.close();
			
			// дата закрытия: 14. Дата расторжения договора если есть то закрываем договор той датой
			if(!findonaddress && abonent.date2 != null)
			{
//				System.out.println( "  close contract date="+abonent.date2+"..." );
				try
				{
					convertUtil.closeContract( contract.getId(), abonent.date2, true );
				}
				catch (BGMessageException e)
				{
					System.out.println("  [!] contract "+abonent.contract+": error close contract: "+e.getMessage());
				}
			}
		}
	}
	
	/** Проверка адресов на заведённость в системе итд */
	private void checkAddress(ConvertUtil convertUtil)
		throws SQLException
	{
		System.out.println( "check address..." );
		Map<String, Map<String, Object>> billingCityMap = convertUtil.getBillingCityMap();
		System.out.println( " loaded city: \"" + Utils.toString(billingCityMap.keySet(),"\", \"") + "\"" );
		Map<Integer, Map<String, Map<String, Object>> > billingStreetMap = convertUtil.getBillingStreetMap();
		Map<Integer, Map<String, Integer>> billingHouseMap = convertUtil.getBillingHouseMap( "" );
		for(BgAbonent abonent : contractList)	
		{
//			System.out.println("["+abonent.street+"|"+abonent.house+"|"+abonent.flat+"]");
			
			if(Utils.isBlankString(abonent.city))
			{
				System.out.println(" [!] empty city, contract: "+abonent.contract);
				continue;
			}
			Map<String, Object> city = billingCityMap.get(abonent.city.toLowerCase());
			if(city == null)
			{
				System.out.println(" [!] not found city '"+abonent.city+"', contract: "+abonent.contract);
				continue;
			}
			// заполняем что нашли города ид
			abonent.cityId = (int)city.get("id");
			
			Map<String, Map<String, Object>> strMap = billingStreetMap.get(abonent.cityId);
			
			if(Utils.isBlankString(abonent.street))
			{
				System.out.println(" [!] empty street, contract: "+abonent.contract);
				continue;
			}
			Map<String, Object> street = strMap != null ? strMap.get(abonent.street.toLowerCase()) : null;
			if(street == null)
			{
				System.out.println(" [!] not found street '"+abonent.street+"' on city '"+abonent.city+"', contract: "+abonent.contract);
				continue;
			}
			// заполняем что нашли улицы ид
			abonent.streetId = (int)street.get("id");
//			System.out.println(" street "+streetId+"|"+street.get("origin_title"));
			// дом
			if(Utils.isBlankString(abonent.house))
			{
				System.out.println(" [!] empty house, contract: "+abonent.contract);
				continue;
			}
			Integer houseId = null;
			Map<String, Integer> housesOfStreet = billingHouseMap.get(abonent.streetId);
			if(housesOfStreet != null)
			{
				houseId = housesOfStreet.get(abonent.house.toLowerCase());
			}
			if(houseId==null)
			{
				System.out.println(" [!] not found house '"+abonent.house+"' on street '"+abonent.street+"', contract: "+abonent.contract);
				continue;
			}
			abonent.houseId = houseId;
		}
	}

	/** загрузка абонентов */
	private void loadAbonent()
		throws Exception
	{
		System.out.println( "load abonent..." );
		List<Map<String, String>> _abonents = ConvertUtil.loadTxtDB(CSV_abonent, "cp1251", ";", CSV_FIELDS_abonent);
		int numline = 0;
		int entry_ignore = 0;
		int entry_error = 0;
		Set<Integer> cnts = new HashSet<Integer>();
		for(Map<String, String> abonent : _abonents)
		{
			++numline;
			// {"gr","parent","contract","fio","street","house","flat","tel","doc","doc_rekv","date1","ct_analog","ct_digit","date2"};
			int gr = Utils.parseInt(abonent.get("gr"), -1);
			int parent = Utils.parseInt(abonent.get("parent"), -1);
			int cnt = Utils.parseInt(abonent.get("contract"), -1);
			String fio = abonent.get("fio");
			String city = abonent.get("city");
			String street = abonent.get("street");
			String house = abonent.get("house");
			String flat = abonent.get("flat");
			String tel = abonent.get("tel");
			String doc = abonent.get("doc");
			String doc_rekv = abonent.get("doc_rekv");
			Date date1 = TimeUtils.parseDate(abonent.get("date1"), CSV_FIELDS_payment_dateformat);
			int ct_analog = Utils.parseInt(abonent.get("ct_analog"), -1);
			int ct_digit = Utils.parseInt(abonent.get("ct_digit"), -1);
			Date date2 = TimeUtils.parseDate(abonent.get("date2"), CSV_FIELDS_payment_dateformat);
			if(gr == -1 || (parent == -1 && gr==0) || cnt == -1 || date1 == null )
			{
				System.out.println(" [!] error abonent line "+numline+": "+abonent);
				++entry_error;
				continue;
			}
			if(gr == 1)
			{
				++entry_ignore;
				continue;
			}
			if(ct_analog != 1)
			{
				System.out.println(" [!] no ct_analog abonent line "+numline+": "+abonent);
				++entry_error;
				continue;
			}
			if(cnts.contains(cnt))
			{
				System.out.println(" [!] dublicate entry for abonent line "+numline+": "+cnt);
				++entry_error;
				continue;
			}
			// 8. Телефон - В 1с два значения для телефона (зачем то) телефон просто и телефон для смс, берем первый номер, заносим в параметр №16(тип поля телефон) из биллинга
			if(Utils.notBlankString(tel))
			{
				try
				{
					Long.valueOf(tel);
					// значит tel уже нормальные цифры
				}
				catch(Exception e1)
				{
					String[] t2 = tel.split(" ");
					try
					{
						Long.valueOf(t2[0]);
						tel = t2[0];
						// значит tel первая часть - нормальные цифры
					}
					catch(Exception e2)
					{
						// значит вообще что-то левое
						System.out.println(" [!] error phone format "+tel);
						tel = null;
					}
				}
			}
			else
			{
				tel = null;
			}
			// КодРодителя
			// Код абонента
			// Ф.И.О.
			// Улица + Дом + Квартира + Телефон
			// Документ + Реквизиты и кем выдан документ
			// Дата заключения договора
			// Наличие цифрового договора
			// Дата расторжения договора
			BgAbonent ab = new BgAbonent(parent, cnt, fio, city, street, house, flat, tel, doc, doc_rekv, date1, ct_digit, date2);
			contractList.add(ab);
			cnts.add(cnt);
			
		}
		System.out.println( " total line "+numline );
		System.out.println( " entry ignore "+entry_ignore );
		System.out.println( " entry error "+entry_error );
		System.out.println( " loaded "+contractList.size()+" abonent entry" );
	}

	/** загрузка комментов */
	private void loadAbonentComment()
		throws Exception
	{
		System.out.println( "load abonent_comment..." );
		List<Map<String, String>> _comments = ConvertUtil.loadTxtDB(CSV_abonent_comment, "cp1251", ";", CSV_FIELDS_abonent_comment);
		int numline = 0;
		for(Map<String, String> comment : _comments)
		{
			++numline;
			int cnt = Utils.parseInt(comment.get("contract"), -1);
			String txt = comment.get("comment");
			if(cnt == -1 || txt == null )
			{
				System.out.println(" [!] error abonent_comment line "+numline+": "+comment);
				continue;
			}
			if(contractCommentMap.containsKey(cnt))
			{
				System.out.println(" [!] dublicate entry for abonent line "+numline+": "+cnt);
				continue;
			}
			contractCommentMap.put(cnt, txt);
		}
		System.out.println( " total line "+numline );
		System.out.println( " loaded "+contractCommentMap.size()+" abonent_comment entry" );
	}
	
	/** загрузка остатков */
	private void loadOstatki()
		throws Exception
	{
		System.out.println( "load ostatki..." );
		List<Map<String, String>> _ostatki = ConvertUtil.loadTxtDB(CSV_ostatki, "cp1251", ";", CSV_FIELDS_ostatki);
		Date dateOstatki = new Date();
		int numline = 0;
		int entry_zero = 0;
		for(Map<String, String> ostatk : _ostatki)
		{
			++numline;
			String fio = ostatk.get("fio");
			int cnt = Utils.parseInt(ostatk.get("contract"), -1);
			BigDecimal sum = Utils.parseBigDecimal(ostatk.get("sum"), null);
			String flagpaycha = ostatk.get("flagpaycha");
			if(numline == 1)
			{
				// первую строку обрабатываем отдельно: {fio=Остатки на дату 31.12.15}
				Matcher m = CSV_FIELDS_ostatki_date_regexp_first_line.matcher(fio);
				if(m.find())
				{
					int dd = Utils.parseInt(m.group(1));
					int mm = Utils.parseInt(m.group(2));
					int yy = Utils.parseInt(m.group(3));
					dateOstatki = new Date(yy+2000-1900, mm-1, dd);
					System.out.println(" date ostatki: "+dateOstatki);
				}
				else
				{
					System.out.println(" [!] error date ostatki line "+numline+": "+fio);
				}
				continue;
			}
			if(fio == null || cnt == -1 || sum == null || flagpaycha == null )
			{
				System.out.println(" [!] error ostatki line "+numline+": "+ostatk);
				continue;
			}
			// если сумма нулевая, то игнорируем просто молча
			if(BigDecimal.ZERO.compareTo(sum) == 0)
			{
				++entry_zero;
				continue;
			}
			BgPayment paymnt = new BgPayment(cnt, dateOstatki, sum, "");
			if(flagpaycha.equalsIgnoreCase(CSV_FIELDS_ostatki_payment))
			{
				addMapList(contractPaymentMap, cnt, paymnt);
			}
			else if(flagpaycha.equalsIgnoreCase(CSV_FIELDS_ostatki_charge))
			{
				addMapList(contractChargeMap, cnt, paymnt);
			}
			else
			{
				System.out.println(" [!] error ostatki line (payment/charge flag) "+numline+": "+ostatk);
				continue;
			}
		}
		System.out.println( " total line "+numline );
		System.out.println( " zero line "+entry_zero );
		System.out.println( " loaded ostatki-payment total "+contractPaymentMap.size()+" contract "+getCountMapList(contractPaymentMap)+" entry" );
		System.out.println( " loaded ostatki-charge total "+contractChargeMap.size()+" contract "+getCountMapList(contractChargeMap)+" entry" );
	}
	
	/** загрузка платежей */
	private void loadPayment()
		throws Exception
	{
		System.out.println( "load payment..." );
		
		List<Map<String, String>> _payments = ConvertUtil.loadTxtDB(CSV_payment, "cp1251", ";", CSV_FIELDS_payment);
		int numline = 0;
		for(Map<String, String> payment : _payments)
		{
			++numline;
			int cnt = Utils.parseInt(payment.get("contract"), -1);
			BigDecimal sum = Utils.parseBigDecimal(payment.get("sum"), null);
			String flagpaycha = payment.get("flagpaycha");
			Date date = TimeUtils.parseDate(payment.get("date"), CSV_FIELDS_payment_dateformat);
			
			String comment1 = payment.get("comment1");
			String comment2 = payment.get("comment2");
			String comment3 = payment.get("comment3");

			if(cnt == -1 || sum == null || flagpaycha == null || date == null)
			{
				System.out.println(" [!] error payment line "+numline+": "+payment);
				continue;
			}

			BgPayment paymnt = new BgPayment(cnt, date, sum, comment1 + " - " + comment2 + " - " + comment3);
			if(flagpaycha.equalsIgnoreCase(CSV_FIELDS_payment_payment))
			{
				addMapList(contractPaymentMap, cnt, paymnt);
			}
			else if(flagpaycha.equalsIgnoreCase(CSV_FIELDS_payment_charge))
			{
				addMapList(contractChargeMap, cnt, paymnt);
			}
			else
			{
				System.out.println(" [!] error payment line (payment/charge flag) "+numline+": "+payment);
				continue;
			}
		}
		System.out.println( " total line "+numline );
		System.out.println( " loaded ostatki-payment total "+contractPaymentMap.size()+" contract "+getCountMapList(contractPaymentMap)+" entry" );
		System.out.println( " loaded ostatki-charge total "+contractChargeMap.size()+" contract "+getCountMapList(contractChargeMap)+" entry" );
	}
	
	/** загрузка карт cerbercypt */
	private void loadDigitcartAbonent()
		throws Exception
	{
		System.out.println( "load digitcart_abonent..." );
		
		List<Map<String, String>> _digitcart_abonents = ConvertUtil.loadTxtDB(CSV_digitcart_abonent, "cp1251", ";", CSV_FIELDS_digitcart_abonent);
		int numline = 0;
		int entry_ignore = 0;
		for(Map<String, String> digitcart_abonent : _digitcart_abonents)
		{
			++numline;
			int cnt = Utils.parseInt(digitcart_abonent.get("contract"), -1);
			String card = digitcart_abonent.get("card");
			String packet = digitcart_abonent.get("packet");
			String status = digitcart_abonent.get("status");
			
			if(cnt == -1 || card == null || packet == null || status == null)
			{
				System.out.println(" [!] error digitcart_abonent line "+numline+": "+digitcart_abonent);
				continue;
			}
			
			if(status.equalsIgnoreCase(CSV_FIELDS_digitcart_abonent_active) || status.equalsIgnoreCase(CSV_FIELDS_digitcart_abonent_suspend))
			{
				if(!card.endsWith("."))
				{
					System.out.println(" [!] no dot-card-number entry digitcart_abonent line "+numline+""+cnt);
					continue;
				}
				long cardnumber;
				try
				{
					cardnumber = Long.parseLong(card.substring(0, card.length()-1));
				}
				catch(Exception ex)
				{
					System.out.println(" [!] error card-number entry digitcart_abonent line "+numline+": "+digitcart_abonent);
					continue;
				}
				
				BgCardPacket cp = new BgCardPacket(cnt, cardnumber, packet);
				addMapList(contractTvSubscription, cnt, cp);
			}
			else if(status.equalsIgnoreCase(CSV_FIELDS_digitcart_abonent_stop))
			{
				// игноринуем
				++entry_ignore;
			}
			else
			{
				System.out.println(" [!] error digitcart_abonent line (status flag) "+numline+": "+digitcart_abonent);
				continue;
			}
		}
		System.out.println( " total line "+numline );
		System.out.println( " entry ignore "+entry_ignore );
		System.out.println( " loaded digitcart_abonen total "+contractTvSubscription.size()+" contract "+getCountMapList(contractTvSubscription)+" entry" );
	}
	
	/** загрузка tarif_abonent */
	private void loadTarifAbonent()
		throws Exception
	{
		System.out.println( "load tarif_abonent..." );
		
		List<Map<String, String>> _tarif_abonents = ConvertUtil.loadTxtDB(CSV_tarif_abonent, "cp1251", ";", CSV_FIELDS_tarif_abonent);	
		
		int numline = 0;
		int entry_ignore = 0;
		for(Map<String, String> tarif_abonent : _tarif_abonents)
		{
			++numline;
			
			int cnt = Utils.parseInt(tarif_abonent.get("contract"), -1);
			String type = tarif_abonent.get("type");
			Date date = TimeUtils.parseDate(tarif_abonent.get("date"), CSV_FIELDS_payment_dateformat);
			String status = tarif_abonent.get("status");
			
			if(tarif_abonent.get("date").equals(".  ."))
			{
				//что делать если даты нет:
				//16180;Аналоговый;06.12.15; . . ;0;Приостановлен;1050;
				//В таком случае абонента не подключили, статус тогда не трогаем, пусть остается который назначен шаблоном
				++entry_ignore;
				continue;
			}

			if(cnt == -1 || type == null || date == null || status == null)
			{
				System.out.println(" [!] error tarif_abonent line "+numline+": "+tarif_abonent);
				continue;
			}
			
			// тип договора - где стоит цифровой пропускаем строку
			if(type.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_analog))
			{
				// noop
			}
			else if(type.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_digit))
			{
				// игноринуем
				++entry_ignore;
				continue;
			}
			else
			{
				System.out.println(" [!] error tarif_abonent line (type) "+numline+": "+tarif_abonent);
				continue;
			}
			
			// статус - текущий статус договора  - если Действует то в bg ставим 0:Активен, если Приоставновлен или Расторгнут то в bg ставим 2:Отключен
			int statuscode = -1;
			if(status.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_stat_active))
			{
				statuscode = CSV_FIELDS_tarif_abonent_setstatus_active_analog;
			}
			else if(type.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_digit) & status.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_stat_active))
			{
				statuscode = CSV_FIELDS_tarif_abonent_setstatus_active_digit;
			}
			else if(status.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_stat_noactive1)||status.equalsIgnoreCase(CSV_FIELDS_tarif_abonent_stat_noactive2))
			{
				statuscode = CSV_FIELDS_tarif_abonent_setstatus_noactive;
			}
			else
			{
				System.out.println(" [!] error tarif_abonent line (status) "+numline+": "+tarif_abonent);
				continue;
			}
			
			// 	Брать дату подключения и проверять если эта дата раньше 1го часла текущего месяца то договор активировать
			// (установить статус договора 0:Активен) 1м часлом текущего месяца, если же дата позже 1го числа текущего месяца
			// то ставить статус активен тем числом что указан в файле.
			Calendar cal1 = Calendar.getInstance();
			cal1.set( Calendar.DAY_OF_MONTH, 1 );
			TimeUtils.clear_HOUR_MIN_MIL_SEC(cal1);
			if(TimeUtils.dateBefore( date, cal1.getTime() ))
			{
				date = cal1.getTime();
			}

			if(contractSetStatusMap.containsKey(cnt))
			{
				System.out.println(" [!] dublicate entry for tarif_abonent "+numline+": "+cnt);
				++entry_ignore;
				continue;
			}
			contractSetStatusMap.put(cnt, new BgSetStatus(cnt, date, statuscode ));
		}
		System.out.println( " total line "+numline );
		System.out.println( " entry ignore "+entry_ignore );
		System.out.println( " loaded tarif_abonent total "+contractSetStatusMap.size()+" entry" );
	}

	private static <K, V> void addMapList(Map<K, List<V>> map, K mapKey, V listElem)
	{
		List<V> list = map.get(mapKey);
		if(list == null)
		{
			list = new ArrayList<V>();
			map.put(mapKey, list);
		}
		list.add(listElem);
	}
	
	private static <K, V> int getCountMapList(Map<K, List<V>> map)
	{
		int count = 0;
		for(Map.Entry<K, List<V>> e : map.entrySet())
		{
			count += e.getValue()!=null ? e.getValue().size() : 0;
		}
		return count;
	}
	
	private static class BgPayment
	{
		int contract;
		Date date;
		BigDecimal sum;
		String comment;
		public BgPayment(int contract, Date date, BigDecimal sum, String comment)
		{
			this.contract = contract;
			this.date = date;
			this.sum = sum;
			this.comment = comment;
		}
	}
	
	private static class BgCardPacket
	{
		int contract;
		long cardnumber;
		String packet;
		public BgCardPacket(int contract, long cardnumber, String packet)
		{
			this.contract = contract;
			this.cardnumber = cardnumber;
			this.packet = packet;
		}
	}

	private static class BgAbonent
	{
		int parent;
		int contract;
		String fio;
		
		String city;
		String street;
		String house;
		String flat;
		String tel;
		
		String doc;
		String doc_rekv;
		
		Date date1;
		int ct_digit;
		Date date2;

		int cityId = -1; // заполняется по пути, сопоставляясь из БД
		int streetId = -1; // заполняется по пути, сопоставляясь из БД
		int houseId = -1; // заполняется по пути, сопоставляясь из БД
		
		public BgAbonent(int parent, int contract, String fio, String city, String street, String house, String flat, String tel, String doc, String doc_rekv, Date date1, int ct_digit, Date date2)
		{
			this.parent = parent;
			this.contract = contract;
			this.fio = fio;
			this.city = city;
			this.street = street;
			this.house = house;
			this.flat = flat;
			this.tel = tel;
			this.doc = doc;
			this.doc_rekv = doc_rekv;
			this.date1 = date1;
			this.ct_digit = ct_digit;
			this.date2 = date2;
		}
	}
	
	private static class BgSetStatus
	{
		int contract;
		Date date;
		int status;
		public BgSetStatus(int contract, Date date, int status)
		{
			this.contract = contract;
			this.date = date;
			this.status = status;
		}
	}

	/** заглушка, которая вызывает метод execute  */
	public static void main(String[] args)
		throws Exception
	{
		Setup setup = new Setup("data.data");
	    Setup.setSetup( setup );
	    
	    ConnectionSet connectionSet = ConnectionSet.newInstance( setup, false );
	    
	    try
        {
	    	System.out.println( "execute start" );
	    	new ConvertContracts().execute(setup, connectionSet);
	    	System.out.println( "execute finish" );
        }
	    catch(Exception ex)
	    {
	    	ex.printStackTrace();
	    }
        finally
        {
            connectionSet.commit();
            System.exit(0);
        }
	}
}
