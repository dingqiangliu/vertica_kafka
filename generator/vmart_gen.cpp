
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>

#ifdef __CYGWIN__
#define getline __getline
#endif /* __CYGWIN__ */

typedef unsigned long long uint64;

uint64 flip(uint64 lb, uint64 hb);
void store_gen(const char *file, uint64 numstores);
void prod_gen(const char *file, uint64 numproducts);
void promo_gen(const char *file, uint64 numpromos);
void vend_gen(const char *file, uint64 numvends);
void cust_gen(const char *file, uint64 numcusts);
void emp_gen(const char *file, uint64 numemps);
void gen_load_script(const char *filename,uint64 numfiles,int pwd);
uint64 time_gen(const char *timefile, const char *outfile);

void warehouse_gen(const char *file, uint64 numwarehouses);
void shipping_gen(const char *file, uint64 numshipping);
void onlinepage_gen(const char *file, uint64 numonlinepages);
void callcenter_gen(const char *file, uint64 numcallcenters);


const char *nullflag;
char loadnull[256]; // will be nullflag plus extra dashes if
                    // necessary for writing to load script

static uint64 *prodVersionArray = NULL;
static uint64 numprods;

#define DEFAULT_TIMEFILE "Time.txt"
#define DEFAULT_NUMPRODKEYS  60000
#define DEFAULT_NUMSTOREKEYS   250
#define DEFAULT_NUMPROMOKEYS  1000
#define DEFAULT_NUMVENDKEYS   50
#define DEFAULT_NUMCUSTKEYS   50000
#define DEFAULT_NUMEMPKEYS 10000
#define DEFAULT_NUMSALESFACTROWS 5000000
#define DEFAULT_NUMORDERFACTROWS 300000
#define DEFAULT_NUMFILES     1
#define DEFAULT_RAND         20177
#define DEFAULT_NULL_VALUE   ""

#define DEFAULT_NUMWAREHOUSEKEYS 100
#define DEFAULT_NUMSHIPPINGKEYS 100
#define DEFAULT_NUMONLINEPAGEKEYS 1000
#define DEFAULT_NUMCALLCENTERKEYS 200

#define DEFAULT_NUMONLINESALESFACTROWS 5000000
#define DEFAULT_NUMINVENTORYFACTROWS 300000

main(int argc, char **argv)
{
  uint64 tid, cid;

  const char *timefile = DEFAULT_TIMEFILE;
  uint64 numprodkeys = DEFAULT_NUMPRODKEYS;
  uint64 numstorekeys = DEFAULT_NUMSTOREKEYS;
  uint64 numpromokeys = DEFAULT_NUMPROMOKEYS;
  uint64 numvendkeys = DEFAULT_NUMVENDKEYS;
  uint64 numcustkeys = DEFAULT_NUMCUSTKEYS;
  uint64 numempkeys = DEFAULT_NUMEMPKEYS;
  uint64 numfactsalesrows = DEFAULT_NUMSALESFACTROWS;
  uint64 numfactorderrows = DEFAULT_NUMORDERFACTROWS;
  uint64 numfiles = DEFAULT_NUMFILES;
  uint64 rand = DEFAULT_RAND;
  nullflag = DEFAULT_NULL_VALUE;

  uint64 numwarehousekeys = DEFAULT_NUMWAREHOUSEKEYS;
  uint64 numshippingkeys = DEFAULT_NUMSHIPPINGKEYS;
  uint64 numonlinepagekeys = DEFAULT_NUMONLINEPAGEKEYS;
  uint64 numcallcenterkeys = DEFAULT_NUMCALLCENTERKEYS;
  uint64 numfactonlinesalesrows = DEFAULT_NUMONLINESALESFACTROWS;
  uint64 numfactinventoryrows = DEFAULT_NUMINVENTORYFACTROWS;
  
  const char *datadirectory="./";
  char path[256];
  int datadirstatus = -1;
  int c;

  bool any = false;
  bool gen_load_flag = false;

  // Add by DQ 
  char *tablename=NULL;
  char *outputfilename=NULL;

 while (1)
 {
   static struct option long_options[] =
	 {
	   {"files", required_argument, 0, 'a'},
	   {"seed", required_argument, 0, 'b'},
	   {"time_file", required_argument, 0, 'c'},
   	   {"store_sales_fact", required_argument, 0, 'd'},
           {"store_orders_fact", required_argument, 0, 'e'},
   	   {"product_dimension", required_argument, 0, 'f'},
   	   {"store_dimension", required_argument, 0, 'g'},
   	   {"promotion_dimension", required_argument, 0, 'h'},
           {"vendor_dimension", required_argument, 0, 'i'},
           {"customer_dimension", required_argument, 0, 'j'},
           {"employee_dimension", required_argument, 0, 'k'},
           {"null", required_argument, 0, 'l'},
   	   {"xxx", required_argument, 0, 'm'},
   	   {"yyy", required_argument, 0, 'n'},
   	   {"zzz", required_argument, 0, 'o'},
   	   {"warehouse_dimension",required_argument, 0, 'p'},
   	   {"shipping_dimension",required_argument, 0, 'q'},
   	   {"online_page_dimension",required_argument, 0, 'r'},
   	   {"callcenter_dimension",required_argument, 0, 's'},
   	   {"online_sales_fact",required_argument, 0, 't'},
   	   {"inventory_fact",required_argument, 0, 'u'},
   	   {"gen_load_script",no_argument, 0, 'v'},
	   {"datadirectory", required_argument, 0, 'w'},
	   // Add by DQ 
	   {"tablename", required_argument, 0, 'x'},
	   {"outputfilename", required_argument, 0, 'y'},

	   {0, 0, 0, 0}
	 };
   /* getopt_long stores the option index here. */
   int option_index = 0;

   c = getopt_long (argc, argv, "",
					long_options, &option_index);

   /* Detect the end of the options. */
   if (c == -1)
	 break;

   any=true;

   switch (c)
	 {
	 case 'a': numfiles = atoll(optarg); break;
	 case 'b': rand = atoll(optarg); break;
	 case 'c': timefile = strdup(optarg); break;
	 case 'd': numfactsalesrows = atoll(optarg); break;
     case 'e': numfactorderrows = atoll(optarg); break;
	 case 'f': numprodkeys = atoll(optarg); break;
	 case 'g': numstorekeys = atoll(optarg); break;
	 case 'h': numpromokeys = atoll(optarg); break;
     case 'i': numvendkeys = atoll(optarg); break;
     case 'j': numcustkeys = atoll(optarg); break;
     case 'k': numempkeys = atoll(optarg); break;
     case 'l': nullflag = optarg; break;
	 case 'm': break;
	 case 'n': break;
	 case 'o': break;
	 case '?': break;
	 case 'p': numwarehousekeys = atoll(optarg); break;
	 case 'q': numshippingkeys = atoll(optarg); break;
	 case 'r': numonlinepagekeys = atoll(optarg); break;
	 case 's': numcallcenterkeys = atoll(optarg); break;
	 case 't': numfactonlinesalesrows = atoll(optarg); break;
	 case 'u': numfactinventoryrows = atoll(optarg); break;
	 case 'v': gen_load_flag = true; break;
	 case 'w': datadirectory = strdup(optarg); break;
     // Add by DQ 
	 case 'x': tablename=strdup(optarg); break;
	 case 'y': outputfilename=strdup(optarg); break;

	 default:
 	   fprintf(stderr, "default\n", optarg);
	   abort ();
	 }
 }



 if (!any) fprintf(stderr, "Using default parameters\n");

/* Print any remaining command line arguments (not options). */
 if (optind < argc)
 {
   fprintf(stderr, "Invalid command line options: ");
   while (optind < argc)
	 fprintf(stderr, "%s ", argv[optind++]);
   fprintf(stderr, "\n");
 }
  
  fprintf(stderr, "datadirectory = %s\n", datadirectory);
  fprintf(stderr, "numfiles = %lld\n", numfiles);
  fprintf(stderr, "seed = %lld\n", rand);
  fprintf(stderr, "null = '%s'\n", nullflag);
  fprintf(stderr, "timefile = %s\n", timefile);
  fprintf(stderr, "numfactsalesrows = %lld\n", numfactsalesrows);
  fprintf(stderr, "numfactorderrows = %lld\n", numfactorderrows);
  fprintf(stderr, "numprodkeys = %lld\n", numprodkeys);
  fprintf(stderr, "numstorekeys = %lld\n", numstorekeys);
  fprintf(stderr, "numpromokeys = %lld\n", numpromokeys);
  fprintf(stderr, "numvendkeys = %lld\n", numvendkeys);
  fprintf(stderr, "numcustkeys = %lld\n", numcustkeys);
  fprintf(stderr, "numempkeys = %lld\n", numempkeys);
  fprintf(stderr, "numwarehousekeys = %lld\n", numwarehousekeys);
  fprintf(stderr, "numshippingkeys = %lld\n", numshippingkeys);
  fprintf(stderr, "numonlinepagekeys = %lld\n", numonlinepagekeys);
  fprintf(stderr, "numcallcenterkeys = %lld\n", numcallcenterkeys);
  fprintf(stderr, "numfactonlinesalesrows = %lld\n", numfactonlinesalesrows);
  fprintf(stderr, "numinventoryfactrows = %lld\n", numfactinventoryrows);

  // Add by DQ 
  fprintf(stderr, "tablename = %s\n", !tablename? "": tablename);
  fprintf(stderr, "outputfilename = %s\n", !outputfilename? "": outputfilename);

  sprintf(path,"mkdir -p %s",datadirectory);
  datadirstatus = system(path);
  if(datadirstatus != 0)
  {
    fprintf(stderr, "Please provide different data directory path.\n");
    exit(1);
  }

  datadirstatus = -1;
  datadirstatus = access(datadirectory,W_OK);
  if(datadirstatus != 0)
  {
     fprintf(stderr, "Please provide different data directory path.\n");
     exit(1);
  }

  char filename[256];

  if (gen_load_flag == true) {
    fprintf(stderr, "gen_load_script = true\n");
    // create the data load script so that it
    // matches the number of split fact table files
	int pwd=1;
    sprintf(filename, "%s/%s", datadirectory,"vmart_load_data.sql"); 
	gen_load_script(filename,numfiles,pwd);
    pwd=0;
	sprintf(filename, "%s/%s", datadirectory,"vmart_load_data_datadir.sql"); 
	gen_load_script(filename,numfiles,pwd);
  }
  else
    fprintf(stderr, "gen_load_script = false\n");

  // Seed the random number generator.
  srand(rand);
  
  // Add by DQ
  if(outputfilename) sprintf(filename, "%s", outputfilename);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Store_Dimension", ".tbl");
  if(!strcasecmp("Store_Dimension", tablename)) store_gen(filename, numstorekeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Date_Dimension", ".tbl");
  uint64 numtimekeys = time_gen(timefile, !strcasecmp("Date_Dimension", tablename)? filename: NULL);
  
  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Product_Dimension", ".tbl");
  prod_gen(!strcasecmp("Date_Dimension", tablename)? filename: NULL, numprodkeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Promotion_Dimension", ".tbl");
  if(!strcasecmp("Promotion_Dimension", tablename)) promo_gen(filename, numpromokeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Vendor_Dimension", ".tbl");
  if(!strcasecmp("Vendor_Dimension", tablename)) vend_gen(filename, numvendkeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Customer_Dimension", ".tbl");
  if(!strcasecmp("Customer_Dimension", tablename)) cust_gen(filename, numcustkeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Employee_Dimension", ".tbl");
  if(!strcasecmp("Employee_Dimension", tablename)) emp_gen(filename, numempkeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Warehouse_Dimension", ".tbl");
  if(!strcasecmp("Warehouse_Dimension", tablename)) warehouse_gen(filename, numwarehousekeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Shipping_Dimension", ".tbl");
  if(!strcasecmp("Shipping_Dimension", tablename)) shipping_gen(filename, numshippingkeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Online_Page_Dimension", ".tbl");
  if(!strcasecmp("Online_Page_Dimension", tablename)) onlinepage_gen(filename, numonlinepagekeys);

  if(!outputfilename) sprintf(filename, "%s/%s%s", datadirectory,"Call_Center_Dimension", ".tbl");
  if(!strcasecmp("Call_Center_Dimension", tablename)) callcenter_gen(filename, numcallcenterkeys);


  // STORE_SALES_FACT
  uint64 numsalesrowsperfile = numfactsalesrows/numfiles;
  uint64 fnum = 1; // file number
  FILE *fd = 0;

  // fprintf(stderr, "POS_Retail_Sales_Transaction_Fact(date_key, product_key, store_key, promo_key, trans_id, quantity, dollar_amount, dollar_cost, profit, trans_type,  trans_time, tender_type)\n");

  uint64 numsalesrows = numfactsalesrows;
  static const char *tendertype[5] =
  {
    "Cash",
    "Credit",
    "Debit",
    "Check",
    "Other"
  };

  if(!strcasecmp("Store_Sales_Fact", tablename))
  while (numsalesrows)
  {
    for (uint64 t = 1; (t <= numtimekeys) && numsalesrows; t++)  // on each day
    {
      uint64 transleft = numsalesrows;
      uint64 keysleft = numtimekeys - t;
      uint64 tpd = transleft / (keysleft + 1);
      uint64 numtrans = flip(0, tpd * 2); // numprods product transactions
      if (keysleft == 0) numtrans = numsalesrows;

      uint64 store, promo, cust, emp, hour, minute, second, tend;

      for (uint64 i = 0; (i < numtrans) && numsalesrows; i++)
      {
        // start new file after numsalesfactrows # of rows.
        // last file may get some extra
        if (((numfactsalesrows - numsalesrows) % numsalesrowsperfile == 0) &&
            fnum <= numfiles)
        {
          if (fd)
            if(fd!=stdout) fclose(fd);

          //char filename[256];
          if (numfiles == 1)
			{
          		if(!outputfilename) sprintf(filename, "%s/%s", datadirectory,"Store_Sales_Fact.tbl"); 
				//sprintf(filename, "Store_Sales_Fact.tbl");
			}
          else
			{
			  if(!outputfilename) sprintf(filename, "%s/%s_%03d.tbl", datadirectory,"Store_Sales_Fact",fnum);
          		//sprintf(filename, "Store_Sales_Fact_%03d.tbl", fnum);
			}
          if(!strcmp("-", filename)) fd=stdout;
		  else fd = fopen(filename, "w");
		  if (fd == NULL)
            {
              fprintf(stderr, "Error : Can't create %s output file !!\n",filename);
              exit(1);
            }
          fnum++;
        }

        uint64 cost = flip(1, 300);
        uint64 profit = flip(5, 300);
        uint64 sametrans = flip(0, 3);
        if(!sametrans || i == 0 || (hour == 23 && minute == 59 && second > 29))
                                     //only change the info sometimes so that
        {                            //a transaction can have the same customer
                                     //buying several different products
          store = flip(1, numstorekeys);
          promo = flip(1, numpromokeys);
          cust = flip(1, numcustkeys);
          emp = flip(1, numempkeys);

          hour = flip(0, 23);
          minute = flip(0, 59);
          second = flip(0, 59);
          tend = flip(0, 4);
        }
        else // same transaction, just increase the time by a small amount
        {
          uint64 newsec = flip(10, 30)  + second;
          if(newsec >= 60)
          {
            if(minute == 59)
            {
              minute = 0;
              hour++; // hour is reset if we're nearing a new day, so no need
            }         // to check that hour isn't 23 before incrementing
            else
              minute++;
          }
          second = newsec % 60;
        }

        const char *transtype;
        uint64 tt = flip(1, 20);
        if(tt > 1)
          transtype = "purchase";
        else
        {
          transtype = "return";
          profit *= -1;
          cost *= -1;
        }

        uint64 prod = flip(1, numprods);

        fprintf(fd, "%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%s",
                t,                     // timekey
                prod,  // product is always different
                flip(1,prodVersionArray[prod]),
                store,
                promo,
                cust,
                emp,
                numfactsalesrows-numsalesrows+1, // transaction number
                flip(1, 10),           // Quantity
                cost + profit,         // sales dollar amount
                cost,                  // cost amount
                profit,                // gross profit
                transtype              // transaction type
               );

        fprintf(fd, "|%02lld:%02lld:%02lld", hour, minute, second );
        fprintf(fd, "|%s\n", tendertype[tend]);

        numsalesrows--;
      }
    }
  }
  if(!strcasecmp("Store_Sales_Fact", tablename))
  if(fd!=stdout) fclose(fd);

  //STORE_ORDER_FACT

  // Now generate the sales_orders_fact table
  uint64 numorderrowsperfile = numfactorderrows/numfiles;
  fnum = 1; // file number
  fd = 0;

  static const char *shippers[10] =
  {
    "American Delivery",
    "Speedy Go",
    "TransFast",
    "Postal Service",
    "Shipping Xperts",
    "FlyBy Shippers",
    "DHX",
    "UPL",
    "Package Throwers",
    "United Express"
  };

  uint64 numorderrows = numfactorderrows;
  if(!strcasecmp("Store_Orders_Fact", tablename))
  while (numorderrows)
  {
    if(((numfactorderrows - numorderrows) % numorderrowsperfile == 0) &&
         fnum <= numfiles)
    {
      if(fd)
        if(fd!=stdout) fclose(fd);

      //char filename[256];
      if(numfiles == 1)
		{
			if(!outputfilename) sprintf(filename, "%s/%s", datadirectory,"Store_Orders_Fact.tbl");    
			//sprintf(filename, "Store_Orders_Fact.tbl", fnum);
		}
      else
		{
			if(!outputfilename) sprintf(filename, "%s/%s_%03d.tbl", datadirectory,"Store_Orders_Fact",fnum);
			//sprintf(filename, "Store_Orders_Fact_%03d.tbl", fnum);
		}
	  if(!strcmp("-", filename)) fd=stdout;
	  else fd = fopen(filename, "w");
	  if (fd == NULL)
            {
              fprintf(stderr, "Error : Can't create %s output file !!\n",filename);
              exit(1);
            }
      fnum++;
    }
    uint64 prod = flip(1, numprods);
    fprintf(fd,"%lld|%lld|%lld|%lld|%lld|%lld",
            prod,
            flip(1, prodVersionArray[prod]),
            flip(1, numstorekeys),
            flip(1, numvendkeys),
            flip(numempkeys/100, numempkeys),
            numfactorderrows-numorderrows+1  // order number
           );
     uint64 omonth = flip(1, 12);
     uint64 oday = flip(1, 10);
     uint64 oyear = flip(2003, 2007);
     uint64 sday = oday + flip(0, 5);
     uint64 dday = sday + flip(3, 10);
     fprintf(fd,"|%lld-%lld-%lld|%lld-%lld-%lld|%lld-%lld-%lld|%lld-%lld-%lld",
             omonth, oday, oyear,     // date ordered
             omonth, sday, oyear,     // date shipped
             omonth, dday, oyear,     // date delivered
             omonth, oday + 7, oyear  // expected delivery date
            );
    uint64 quantity = flip(5, 100);
    uint64 lost = flip(1, 40);
    if(lost > 4) lost = 0;

    fprintf(fd,"|%lld|%lld",
            quantity,
            quantity - lost
           );

    uint64 price = flip(1, 300);
    uint64 shipping = flip(10, 200);
    fprintf(fd,"|%s|%lld|%lld|%lld|%lld|%lld|%lld\n",
            shippers[flip(0, 9)],
            price,
            shipping,
            price * quantity + shipping,
            flip(10, 100),            // quantity in stock
            flip(10, 50),             // reorder level
            flip(50, 150)             // overstock ceiling
           );
     numorderrows--;
  }
  if(!strcasecmp("Store_Orders_Fact", tablename))
  if(fd!=stdout) fclose(fd);

  //INVENTORY_FACT
  uint64 numinventoryrowsperfile = numfactinventoryrows/numfiles;
  fnum = 1; // file number
  fd = 0;

  uint64 numinventoryrows = numfactinventoryrows;

  if(!strcasecmp("Inventory_Fact", tablename))
  while (numinventoryrows)
  {
    for (uint64 t = 1; (t <= numtimekeys) && numinventoryrows; t++)  // on each day
    {
      uint64 transleft = numinventoryrows;
      uint64 keysleft = numtimekeys - t;
      uint64 tpd = transleft / (keysleft + 1);
      uint64 numtrans = flip(0, tpd * 2); // numprods product transactions
      if (keysleft == 0) numtrans = numinventoryrows;

      for (uint64 i = 0; (i < numtrans) && numinventoryrows; i++)
      {
        // start new file after numsalesfactrows # of rows.
        // last file may get some extra
        if (((numfactinventoryrows - numinventoryrows) % numinventoryrowsperfile == 0) &&
            fnum <= numfiles)
        {
          if (fd)
            if(fd!=stdout) fclose(fd);

          //char filename[256];
          if (numfiles == 1)
			{
          		if(!outputfilename) sprintf(filename, "%s/%s", datadirectory,"Inventory_Fact.tbl");   
				//sprintf(filename, "Inventory_Fact.tbl");
			}
          else
			{
          		if(!outputfilename) sprintf(filename, "%s/%s_%03d.tbl", datadirectory,"Inventory_Fact",fnum);
				//sprintf(filename, "Inventory_Fact_%03d.tbl", fnum);
			}
		  if(!strcmp("-", filename)) fd=stdout;
		  else fd = fopen(filename, "w");
		  if (fd == NULL)
            {
              fprintf(stderr, "Error : Can't create %s output file !!\n",filename);
              exit(1);
            }
          fnum++;
        }


		uint64 prod = flip(1, numprods);
        fprintf(fd, "%lld|%lld|%lld|%lld|%lld\n",
                t,                     // timekey
                prod,  // product is always different
                flip(1,prodVersionArray[prod]),
                flip(1, numwarehousekeys),
                flip(1,1000)
               );

        numinventoryrows--;
      }
    }
  }
  if(!strcasecmp("Inventory_Fact", tablename))
  if(fd!=stdout) fclose(fd);


  //ONLINE_SALES_FACT
  uint64 numonlinesalesrowsperfile = numfactonlinesalesrows/numfiles;
  fnum = 1; // file number
  fd = 0;

  uint64 numonlinesalesrows = numfactonlinesalesrows;

  if(!strcasecmp("Online_Sales_Fact", tablename))
  while (numonlinesalesrows)
  {
    for (uint64 t = 1; (t <= numtimekeys) && numonlinesalesrows; t++)  // on each day
    {
      uint64 transleft = numonlinesalesrows;
      uint64 keysleft = numtimekeys - t;
      uint64 tpd = transleft / (keysleft + 1);
      uint64 numtrans = flip(0, tpd * 2); // numprods product transactions
      if (keysleft == 0) numtrans = numonlinesalesrows;

      for (uint64 i = 0; (i < numtrans) && numonlinesalesrows; i++)
      {
        // start new file after numsalesfactrows # of rows.
        // last file may get some extra
        if (((numfactonlinesalesrows - numonlinesalesrows) % numonlinesalesrowsperfile == 0) &&
            fnum <= numfiles)
        {
          if (fd)
            if(fd!=stdout) fclose(fd);

          //char filename[256];
          if (numfiles == 1)
			{
          		if(!outputfilename) sprintf(filename, "%s/%s", datadirectory,"Online_Sales_Fact.tbl");
				//sprintf(filename, "Online_Sales_Fact.tbl");
			}
          else
			{
          		if(!outputfilename) sprintf(filename, "%s/%s_%03d.tbl", datadirectory,"Online_Sales_Fact",fnum);
				//sprintf(filename, "Online_Sales_Fact_%03d.tbl", fnum);
			}
		  if(!strcmp("-", filename)) fd=stdout;
		  else fd = fopen(filename, "w");
		  if (fd == NULL)
            {
              fprintf(stderr, "Error : Can't create %s output file !!\n",filename);
              exit(1);
            }
          fnum++;
        }

        uint64 cost = flip(1, 300);
        uint64 profit = flip(5, 300);
        uint64 ship_amount = flip(5,15);

        const char *transtype;
        uint64 tt = flip(1, 20);
        if(tt > 1)
          transtype = "purchase";
        else
        {
          transtype = "return";
          profit *= -1;
          cost *= -1;
          ship_amount *=-1;
        }

		uint64 prod = flip(1, numprods);

		uint64 shipdatekey;
		if(keysleft <= 5)
			shipdatekey = t;
		else
			shipdatekey = t + flip(1, 5);

        fprintf(fd, "%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%lld|%s\n",
                t,                     // timekey
                shipdatekey,		//ship_date_key
                prod,  // product is always different
                flip(1,prodVersionArray[prod]),
                flip(1, numcustkeys),
                flip(1, numcallcenterkeys),
                flip(1, numonlinepagekeys),
                flip(1, numshippingkeys),
                flip(1, numwarehousekeys),
                flip(1, numpromokeys),
                numfactonlinesalesrows-numonlinesalesrows+1, // transaction number
                flip(1, 10),           // Quantity
                cost + profit,         // sales dollar amount
                ship_amount,					//ship dollar amount
                cost + profit + ship_amount,                  // net dollar amount
                cost,                // cost dollar amount
 				profit,           		              // gross profit dollar amount
 				transtype
               );

        numonlinesalesrows--;
      }
    }
  }
  if(!strcasecmp("Online_Sales_Fact", tablename))
  if(fd!=stdout) fclose(fd);
  fprintf(stderr, "Data Generated successfully !\n");
  exit(0);

  if(prodVersionArray!=NULL)
   	free(prodVersionArray);

}

uint64 flip(uint64 lb, uint64 hb)
{
  return lb + rand() % (hb - lb + 1);
}

typedef struct
{
  const char *city;
  const char *state;
  const char *region;
} Location;

#define NUM_LOC 110
static Location locations[] =
  {
    {"New York"      ,"NY", "East"},
    {"Los Angeles"   ,"CA", "West"},
    {"Chicago"       ,"IL" , "MidWest"},
    {"Houston"       ,"TX" , "South"},
    {"Philadelphia"  ,"PA", "East"},
    {"Phoenix"       ,"AZ", "SouthWest"},
    {"San Diego"     ,"CA", "West"},
    {"Dallas"        ,"TX", "South"},
    {"San Antonio"   ,"TX", "South"},
    {"Detroit"       ,"MI" , "MidWest"},
    {"San Jose"      ,"CA", "West"},
    {"Indianapolis"  ,"IN", "MidWest"},
    {"San Francisco" ,"CA", "West"},
    {"Jacksonville"  ,"FL", "South"},
    {"Columbus"      ,"OH" , "MidWest"},
    {"Austin"        ,"TX", "South"},
    {"Baltimore"     ,"MD", "East"},
    {"Memphis"       ,"TN", "East"},
    {"Milwaukee"     ,"WI", "MidWest"},
    {"Boston","MA", "East"},
    {"Las Vegas","NV", "SouthWest"},
    {"Washington","DC","East"},
    {"Nashville","TN", "East"},
    {"El Paso","TX", "South"},
    {"Seattle","WA", "NorthWest"},
    {"Denver","CO", "SouthWest"},
    {"Charlotte","NC", "East"},
    {"Fort Worth","TX", "South"},
    {"Portland","OR", "NorthWest"},
    {"Pasadena","CA", "West"},
    {"Escondido","CA",  "West"},
    {"Sunnyvale","CA", "West"},
    {"Savannah","GA", "South"},
    {"Fontana","CA", "West"},
    {"Orange","CA", "West"},
    {"Naperville","IL", "MidWest"},
    {"Alexandria","VA", "East"},
    {"Rancho Cucamonga","CA", "West"},
    {"Grand Prairie","TX", "South"},
    {"Fullerton","CA", "West"},
    {"Corona","CA", "West"},
    {"Flint","MI", "MidWest"},
    {"Mesquite","TX", "South"},
    {"Sterling Heights","MI", "East"},
    {"Sioux Falls","SD", "MidWest"},
    {"New Haven","CT", "East"},
    {"Topeka","KS", "SouthWest"},
    {"Concord","CA", "West"},
    {"Evansville","IN", "MidWest"},
    {"Hartford","CT", "East"},
    {"Fayetteville","NC", "East"},
    {"Cedar Rapids","IA", "MidWest"},
    {"Elizabeth","NJ", "East"},
    {"Lansing","MI", "MidWest"},
    {"Lancaster","CA", "West"},
    {"Fort Collins","CO", "SouthWest"},
    {"Coral Springs","FL", "South"},
    {"Stamford","CT", "East"},
    {"Thousand Oaks","CA", "West"},
    {"Vallejo","CA", "West"},
    {"Palmdale","CA", "West"},
    {"Columbia","SC", "East"},
    {"El Monte","CA", "West"},
    {"Abilene","TX", "South"},
    {"North Las Vegas","NV", "SouthWest"},
    {"Ann Arbor","MI", "MidWest"},
    {"Beaumont","TX", "South"},
    {"Waco","TX", "South"},
    {"Independence","MS", "South"},
    {"Peoria","IL", "MidWest"},
    {"Inglewood","CA", "West"},
    {"Springfield","IL", "MidWest"},
    {"Simi Valley","CA", "West"},
    {"Lafayette","LA", "South"},
    {"Gilbert","AZ", "SouthWest"},
    {"Carrollton","TX", "South"},
    {"Bellevue","WA", "NorthWest"},
    {"West Valley City","UT", "West"},
    {"Clearwater","FL", "South"},
    {"Costa Mesa","CA", "West"},
    {"Peoria","AZ", "SouthWest"},
    {"South Bend","IN", "MidWest"},
    {"Downey","CA", "West"},
    {"Waterbury","CT", "East"},
    {"Manchester","NH", "East"},
    {"Allentown","PA", "East"},
    {"McAllen","TX", "South"},
    {"Joliet","IL", "MidWest"},
    {"Lowell","MA", "East"},
    {"Provo","UT", "West"},
    {"West Covina","CA", "West"},
    {"Wichita Falls","TX", "South"},
    {"Erie","PA", "East"},
    {"Daly City","CA", "West"},
    {"Clarksville","TN", "East"},
    {"Norwalk","CA", "West"},
    {"Gary","IN", "MidWest"},
    {"Berkeley","CA", "West"},
    {"Santa Clara","CA", "West"},
    {"Green Bay","WI", "MidWest"},
    {"Cape Coral","FL", "South"},
    {"Arvada","CO", "SouthWest"},
    {"Pueblo","CO", "SouthWest"},
    {"Athens","GA", "South"},
    {"Cambridge","MA", "East"},
    {"Westminster","CO", "SouthWest"},
    {"Ventura","CA", "West"},
    {"Portsmouth","VA", "East"},
    {"Livonia","MI", "MidWest"},
    {"Burbank","CA", "West"}
  };

static const char *streets[20] =
  {
    "Main St", "School St", "Mission St", "Railroad St", "Market St",
    "Bakers St", "Church St", "Essex St", "Brook St", "Lake St",
    "Elm St", "Maple St", "Pine St", "Hereford Rd",  "Cherry St",
    "Alden Ave", "Humphrey St","Davis Rd", "Green St","Kelly St"
  };

static const char *mname[] =
  {
    "Seth", "Doug", "Duncan", "Joseph", "Thom",
    "Jose", "Sam", "Alexander", "David", "John",
    "Michael","James","Steve","Kevin","Luigi",
    "Ben", "Jack", "Robert", "Raja", "Harold",
    "Matt", "Lucas", "Marcus", "Dean", "Craig",
    "Theodore", "Brian", "Mark", "Daniel", "William"
  };
static const char *fname[] =
  {
    "Samantha", "Sarah", "Rebecca", "Sally", "Tanya",
    "Darlene", "Amy", "Jessica", "Linda", "Barbara",
    "Carla", "Juanita", "Sharon", "Mary", "Ruth",
    "Wendy", "Anna", "Laura", "Lauren", "Kim",
    "Emily", "Meghan", "Joanna", "Lily", "Martha",
    "Tiffany", "Alexandra", "Midori", "Betty", "Julie"
  };
static const char *midinit[27] =
  {
    "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O",
    "P","Q","R","S","T","U","V","W","X","Y","Z",""
  };

static const char *surname[50] =
  {
    "Vu", "Li", "Smith", "Martin", "Roy",
    "Brown", "Williams", "Peterson", "Garnett", "Taylor",
    "Farmer", "Perkins", "Jefferson", "Gauthier", "Fortin",
    "Campbell", "Garcia", "Rodriguez", "Moore", "Sanchez",
    "Young", "Lewis", "Harris", "Weaver", "Nguyen",
    "Bauer", "Nielson","Lampert", "Vogel","Miller",
    "Wilson","Robinson", "Carcetti","Jones","Goldberg",
    "Overstreet","Meyer","Webber","King","Pavlov",
    "Dobisz", "Lang","Stein","McCabe","McNulty",
    "Kramer","Winkler","Greenwood", "Jackson","Reyes"
  };

static const char *title[5] =
  {"Mr.","Sir","Dr.","Mrs.","Ms."};

void store_gen(const char *file, uint64 numstores)
{
  static const char *photoTypes[5] = {  "Premium", "1 hr", "24 hr", "DIY", "None" };
  static const char *finTypes[5] = {  "Bank", "ATM", "CheckCashing",
                                "Mortgage", "None" };

  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fd=fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Can't create output file.\n");
      exit(1);
  }

  // fprintf(stderr, "Store_Dimension(store_key,store_name,store_number,street_address,city,state,region,floor_plan,photo,financial_srv,sell_sq_footage,sq_footage,first-open, last-remodel\n");

  for (uint64 i = 1; i <= numstores; i++)
  {
    fprintf(fd,"%lld", i);
    fprintf(fd,"|Store%lld", i);
    fprintf(fd,"|%lld", i);
    fprintf(fd,"|%lld %s", flip(0, 500), streets[flip(0,19)]);

    uint64 loc = flip(0, NUM_LOC-1);

    fprintf(fd,"|%s", locations[loc].city);
    fprintf(fd,"|%s", locations[loc].state);
    fprintf(fd,"|%s", locations[loc].region);
    fprintf(fd,"|Plan%lld", i%5);
    fprintf(fd,"|%s", photoTypes[flip(0,4)]);
    fprintf(fd,"|%s", finTypes[flip(0,4)]);

    uint64 sqfoot = flip(1, 10) * 1000;
    fprintf(fd,"|%lld", (int)(sqfoot * flip(1, 5) * 0.1));
    fprintf(fd,"|%lld", sqfoot);

    uint64 mmonth = flip(1, 6);
    uint64 myear = flip(2003, 2007);
    uint64 mday = 1;

    fprintf(fd, "|%lld-%lld-%lld", mmonth, mday, myear);

    uint64 remodel = flip(1, 4);

    if (myear + remodel <= 2007)
      fprintf(fd, "|%lld-%lld-%lld", mmonth+remodel, mday, myear+remodel);
    else
      fprintf(fd, "|%s", nullflag);

    fprintf(fd,"|%lld", flip(10, 50)); // number of employees
    fprintf(fd,"|%lld", flip(10000, 30000)); // annual shrinkage
    fprintf(fd,"|%lld", flip(5, 500)); // foot traffic
    fprintf(fd,"|%lld", flip(100, 1000) + sqfoot); // monthly rent cost

    fprintf(fd,"\n");
  }
  if(fd!=stdout) fclose(fd);
}

void prod_gen(const char *file, uint64 numproducts)
{
  static const char *dept[] =
    {
      "Frozen Goods",
      "Produce",
      "Bakery",
      "Canned Goods",
      "Meat",
      "Dairy",
      "Seafood",
      "Liquor",
      "Cleaning supplies",
      "Medical",
      "Pharmacy",
      "Photography",
      "Gifts"
    };

#define NUM_PRODS 16
  static const char *prod[13][NUM_PRODS] =
  { // frozen goods
    {"frozen pizza", "frozen ravioli","chocolate ice cream",
     "vanilla ice cream", "strawberry ice cream", "frozen juice",
     "fish sticks", "frozen stuffed chicken", "frozen french fries",
     "ice cream sandwhiches", "tv dinner", "frozen bagels",
     "frozen chicken patties", "frozen spinach", "frozen onion rings", "ice"
    },
    // produce
    {"apples", "bananas", "watermelon", "strawberries",
     "tomatoes", "broccolli", "lettuce", "potatoes",
     "red peppers", "green peppers", "cucumbers", "oranges",
     "onions", "cantaloupe", "squash", "garlic"
    },
    // bakery
    {"french bread", "coffee cake", "hamburger buns", "hotdog buns",
     "english muffins", "blueberry muffins", "corn muffins", "croissants",
     "jelly donuts","chocolate chip cookies","peanut butter cookies","bagels",
     "italian bread", "wheat bread", "white bread", "cinnamon buns"
    },
    // canned goods
    {"chicken noodle soup", "vegatable soup", "minestrone soup", "canned tuna",
     "canned chili", "canned tomatoes", "baked beans", "kidney beans",
     "fruit cocktail", "canned peaches", "canned corn", "canned olives",
     "canned green beans", "lima beans", "sardines", "canned chicken broth"
    },
    // meat
    {"steak", "tenderloin", "lamb", "ground beef",
     "sausage", "sliced turkey", "glazed ham", "rotisserie chicken",
     "chicken nuggets", "chicken patties", "veal", "pork",
     "bacon", "hot dogs", "chicken wings", "bratwurst"
    },
    // dairy
    {"whole milk", "low fat milk", "eggs", "butter",
     "cheddar cheese", "mozzarella cheese", "american cheese", "yogurt",
     "whipped cream", "skim milk", "lactose free milk", "half and half",
     "grated parmesan cheese", "sour cream", "margarine", "butter milk"
    },
    // seafood
    {"shrimp", "lobster", "salmon", "tuna",
     "cod", "halibut", "swordfish", "sushi",
     "fried fish patties", "crab cakes", "scallops", "clams",
     "crab legs", "flounder", "haddock", "catfish"
    },
    // liquor
    {"beer", "red wine", "white wine", "vodka",
     "whiskey", "scotch", "tequila", "gin",
     "rum", "brandy", "bourbon", "champagne",
     "light beer", "ale", "hard lemonade", "hard iced tea"
    },
    // cleaning supplies
    {"dish soap", "brooms", "mops", "laundry detergent",
     "fabric softener", "air freshener", "bleach", "sponges",
     "paper towels", "trash bags", "oven cleaner", "glass cleaner",
     "rubber gloves", "feather duster", "starch", "silver polishing cream"
    },
    // medical
    {"band aids", "baby powder", "first aid kit", "sling",
     "wrap bandage", "crutches", "cold remedy", "thermometer",
     "nasal spray", "air humidifier", "air purifier", "splint",
     "wheechair", "rash ointment", "heating pad", "orthopedic brace"
    },
    // pharmacy
    {"antibiotics", "allergy pills", "pain killers", "asthma inhaler",
     "sleeping pills", "nicotine gum", "nicotine patches", "diabetes blood testing kit",
     "cough syrup", "blood pressure medicine", "cholesterol reducer", "birth control",
     "vapor rub", "ulcer medication", "decongestant", "heart medication"
    },
    // photography
    {"digital camera", "telephoto lens", "film", "memory card",
     "analog camera", "camera case", "camera strap", "photography paper",
     "camera batteries", "picture frames", "photo albums", "lens cap",
     "cables", "adapters", "camera cleaner kit", "lens cleaner"
    },
    // gifts
    {"greeting cards", "birthday cards", "watch", "ring",
     "bracelet", "box of candy", "stuffed animal", "flowers",
     "perfume", "electric razor", "beard trimmer", "football",
     "basketball", "tennis racket", "fishing pole", "golf clubs"
    }
  };
  static const char *cat[] =
    {
      "Food",
      "Food",
      "Food",
      "Food",
      "Food",
      "Food",
      "Food",
      "Non-food",
      "Non-food",
      "Medical",
      "Medical",
      "Misc",
      "Misc"
    };

#define NUM_FOODS 7

  static const char *package[] =
    {
      "Bottle",
      "Box",
      "Bag",
      "Other"
    };
  static const char *pkgsize[] =
    {
      "12 oz",
      "18 oz",
      "1 litre",
      "1 gallon",
      "Economy",
      "Family",
      "Other",
    };
  static const char *diet[] =
    {
      "Atkins",
      "Weight Watchers",
      "Light",
      "Low Fat",
      "South Beach",
      "Zone",
      "N/A"
    };
  static const char *units[] = { "pound", "ounce", "gram" };

  FILE *fd = NULL;
  if(file) {
	if(!strcmp("-", file)) fd=stdout;
	else fd=fopen(file, "w");
    if (fd == NULL)
    {
      fprintf(stderr, "Error : Can't create Product_Dimension output file !!\n");
      exit(1);
    }
  }

  // fprintf(stderr, "Product_Dimension(product_key|description|sku|cat|dept|package|pkgsize|fat|diet|wt|wtunits|shelf_width|shelf_ht|shelf_depth)\n");

  prodVersionArray = (uint64 *)malloc(sizeof(uint64)  * numproducts);

  if(prodVersionArray == NULL)
  		printf("Unable to allocate memory for prodVersionArray. Using version 1 for all products.");

  uint64 prodkey=1;
  uint64 prodversion=1;

  for (uint64 i = 1; i <= numproducts; i++)
  {
    uint64 d = flip(0, 12);

    if(prodversion==1)
    {
		prodversion = flip(1,5);
		if(i!=1)
			prodkey++;
		prodVersionArray[prodkey]=prodversion;
	}
	else
	{
		prodversion--;
	}
	
	if(file) {
      fprintf(fd,"%lld", prodkey);
      fprintf(fd,"|%lld", prodversion);
      fprintf(fd,"|Brand #%lld %s", i, prod[d][flip(0, NUM_PRODS-1)]);
      fprintf(fd,"|SKU-#%lld", i);
      fprintf(fd,"|%s", cat[d]);
      fprintf(fd,"|%s", dept[d]);

      if (d < NUM_FOODS)
      {
        fprintf(fd,"|%s", package[flip(0,3)]);
        fprintf(fd,"|%s", pkgsize[flip(0,6)]);
        fprintf(fd,"|%lld", flip(80, 90));
        fprintf(fd,"|%s", diet[flip(0,6)]);
        fprintf(fd,"|%lld", flip(1, 100));
        fprintf(fd,"|%s", units[flip(0, 2)]);
      }
      else
      {
        fprintf(fd,"|Other");
        fprintf(fd,"|Other");
        fprintf(fd,"|0");
        fprintf(fd,"|N/A");
        fprintf(fd,"|%lld", flip(1, 100));
        fprintf(fd,"|pound");
      }
      fprintf(fd,"|%lld", flip(1, 5));
      fprintf(fd,"|%lld", flip(1, 5));
      fprintf(fd,"|%lld", flip(1, 5));

      uint64 cost = flip(1, 300);
      uint64 profit = flip(5, 300);
      uint64 price = cost + profit;
      uint64 lcp = price - flip(0, price/10);
      uint64 hcp = price + flip(0, price/10);
      uint64 acp = (lcp + hcp)/2;

      fprintf(fd,"|%lld|%lld|%lld|%lld|%lld",
              price,   // product price
              cost,    // product cost
              lcp,     // lowest competitor price
              hcp,     // highest competitor price
              acp      // average competitor price
              );
      uint64 disc = flip(1, 50); //discontinued item (only 1 out of 50)
      if(disc > 1)
        fprintf(fd, "|%i\n", 0);
      else
        fprintf(fd, "|%i\n", 1);
	}
  }
  prodVersionArray[prodkey]=prodversion;
  numprods = prodkey - 1;
  if(file) if(fd!=stdout) fclose(fd);
}

void promo_gen(const char *file, uint64 numpromos)
{
  static const char *season[] =
    {
      "Summer", "Winter", "July 4th", "Thanksgiving", "Christmas"
    };
  static const char *sale[] =
    {
      "Super", "Discount", "Liquidation", "Cool", "Mega"
    };
  static const char *sale2[] =
    {
      "Sale", "Sellathon", "Promotion"
    };
  static const char *reduction[] =
    {
      "Half Price", "3 for price of 2", "50 Cents off", "Two for one", "20-50 percent off"
    };

  static const char *media[]=
    {
      "Newspaper", "Magazine", "Radio", "TV", "Billboard", "Coupon", "Online"
    };

  static const char *adtype[] =
    {
      "Fullpage", "Halfpage", "1 minute", "30 seconds"
    };

  static const char *display[]=
    {
      "Kiosk", "End Aisle", "Shelf", "POS"
    };
  static const char *coupon[] =
    {
      "Post", "Email", "Register Receipt", "Pennysaver", "Phone book"
    };
  static const char *admedia[] =
    {
      "Time Magazine", "Superbowl Spot", "Boston Globe", "Other"
    };
  static const char *displayprovider[] =
    {
      "Corporate", "Manufacturer", "Wholesaler", "Other"
    };

  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fd=fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Promotion_Dimension output file !!\n");
      exit(1);
  }

  // fprintf(stderr, "Promotion_Dimension(promo_key|name|reduction|media|adtype|display|coupon|admedia|dispprov|cost|begin_date|end_date)\n");

  for (uint64 i = 1; i <= numpromos; i++)
  {
    fprintf(fd,"%lld", i);
    uint64 s = flip(0, 4);
    fprintf(fd,"|%s %s %s", season[s], sale[flip(0,4)], sale2[flip(0,2)]);
    fprintf(fd,"|%s", reduction[flip(0,4)]);
    fprintf(fd,"|%s", media[flip(0,6)]);
    fprintf(fd,"|%s", adtype[flip(0,3)]);
    fprintf(fd,"|%s", display[flip(0,3)]);
    fprintf(fd,"|%s", coupon[flip(0,4)]);
    fprintf(fd,"|%s", admedia[flip(0,3)]);
    fprintf(fd,"|%s", displayprovider[flip(0,3)]);
    fprintf(fd,"|%lld", flip(100, 500));

    //summer, winter, july 4th, thanksgiving, christmas
    uint64 month;
    if(s == 0) month = flip(5, 8);
    else if(s == 1) {
      uint64 y = flip(0, 1);
      if(y) month = flip(11, 12);
      else  month = flip(1, 2);
    }
    else if(s == 2)  month = 7;
    else if(s == 3)  month = 11;
    else month = 12; //s == 4

    uint64 day = flip(1, 14);
    uint64 year = flip(2003, 2007);

    fprintf(fd,"|%lld-%lld-%lld", month, day, year);
    fprintf(fd,"|%lld-%lld-%lld", month, day + flip(1, 14), year);
    fprintf(fd,"\n");
  }
  if(fd!=stdout) fclose(fd);
}

void vend_gen(const char *file, uint64 numvends)
{

  FILE *fd = fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Vendor_Dimension output file !!\n");
      exit(1);
  }

  static const char *vendor[] =
  {
    "Food",
    "Market",
    "Deal",
    "Frozen",
    "Delicious",
    "Ripe",
    "Big Al's",
    "Sundry",
    "Everything"
  };
  static const char *vendor2[] =
  {
    "Wholesale",
    "Market",
    "Suppliers",
    "Mart",
    "Warehouse",
    "Farm",
    "Depot",
    "Outlet",
    "Discounters",
    "Emporium"
  };

  for(uint64 i = 1; i <= numvends; i++)
  {
    uint64 loc = flip(0, NUM_LOC-1);
    fprintf(fd,"%lld|%s %s|%lld %s|%s|%s|%s|%lld|%lld-%lld-%lld\n",
             i,                                            //vendor key
             vendor[flip(0, 8)], vendor2[flip(0, 9)],      //vendor name
             flip(1, 500), streets[flip(0, 19)],           //street address
             locations[loc].city,
             locations[loc].state,
             locations[loc].region,
             flip(500, 1000000),                           //deal size
             flip(1, 12), flip(1, 28), flip(2003, 2007)    // last deal update
            );
  }
  if(fd!=stdout) fclose(fd);
}

void cust_gen(const char *file, uint64 numcusts)
{
  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Customer_Dimension output file !!\n");
      exit(1);
  }

  static const char *marital[7] =
  {
    "Single",
    "Engaged",
    "Married",
    "Divorced",
    "Separated",
    "Widowed",
    "Unknown"
  };
  static const char *company[11] =
  {
    "Ameri",
    "Veri",
    "Ini",
    "Virta",
    "Better",
    "Ever",
    "Info",
    "Gold",
    "Food",
    "Meta",
    "Intra"
  };
  static const char *company2[11] =
  {
    "core",
    "care",
    "corp",
    "tech",
    "hope",
    "com",
    "shop",
    "gen",
    "data",
    "media",
    "star"
  };
  static const char *occupation[35] =
  {
    "Retired","Farmer","Mechanic","Accountant", "Software Developer",
    "Student","Painter","Chemist","Stock Broker","Psychologist",
    "Writer", "Acrobat","Teacher","Banker","Chef",
    "Hairdresser","Waiter","Police Officer","Priest", "Rabbi",
    "Dancer","Unemployed","Doctor","Exterminator","Jeweler",
    "Detective","Blacksmith","Cobbler","Fisherman","Lumberjack",
    "Butler","Meteorologist","Musician","Actor", "Other"
  };
  static const char *stage[6] =
  {
    "Prospect",
    "Negotiations",
    "Presented Solutions",
    "Collecting Requirements",
    "Closed - Lost",
    "Closed - Won"
  };


  for(uint64 i = 1; i <= numcusts; i++)
  {
    fprintf(fd,"%lld", i); //customer key
    uint64 loc = flip(0, NUM_LOC-1);
    uint64 t = flip(1, 30);

    if(t > 1)  // customer is a single person
    {
      fprintf(fd,"|%s", "Individual");
      uint64 gend = flip(0, 1);
      if(gend) {
        fprintf(fd,"|%s %s. %s", mname[flip(0, 29)],
                                midinit[flip(0, 26)],
                                surname[flip(0, 49)]);
        fprintf(fd,"|%s", "Male");
        fprintf(fd,"|%s", title[flip(0, 2)]);
      }
      else {
        fprintf(fd,"|%s %s. %s", fname[flip(0, 29)],
                                midinit[flip(0, 26)],
                                surname[flip(0, 49)]);
        fprintf(fd,"|%s", "Female");
        fprintf(fd,"|%s", title[flip(2, 4)]);
      }
      fprintf(fd,"|%lld", flip(1, 30000)); // household id

      fprintf(fd,"|%lld %s|%s|%s|%s|%s|%lld|%lld|%lld|%s|%lld|%lld",
              flip(1, 500), streets[flip(0, 19)],
              locations[loc].city,
              locations[loc].state,
              locations[loc].region,
              marital[flip(0, 6)],
              flip(18, 70),     // age
              flip(0, 5),       // number of children
              flip(10000, 1000000),  // annual income
              occupation[flip(0, 33)],
              flip(20, 1000),   // maximum bill amount
              flip(0, 1)        // store membership card
             );
      // customer since
      fprintf(fd,"|%lld-%lld-%lld", flip(1, 12), flip(1, 28), flip(1965, 2007));
      fprintf(fd, "|%s|%s|%s", nullflag, nullflag, nullflag);
    }
    else    // customer is a company
    {
      fprintf(fd,"|%s", "Company");
      fprintf(fd,"|%s%s", company[flip(0, 10)],   // company name
                          company2[flip(0, 10)]);

      // company has no title, gender, or household
      fprintf(fd, "|%s|%s|%s", nullflag, nullflag, nullflag);

      uint64 age = flip(1, 100);
      fprintf(fd,"|%lld %s|%s|%s|%s",
              flip(1, 500), streets[flip(0, 19)],
              locations[loc].city,
              locations[loc].state,
              locations[loc].region);
      //company has no marital status
      fprintf(fd, "|%s", nullflag);
      fprintf(fd, "|%lld", age);
      //company has no # of children
      fprintf(fd, "|%s", nullflag);
      fprintf(fd, "|%lld", flip(500000, 100000000)); // company annual income
      //company has no occupation
      fprintf(fd, "|%s", nullflag);
      fprintf(fd, "|%lld|%lld",
              flip(1000, 500000),         // maximum bill amount
              flip(0, 1)                  // store membership card
             );
      fprintf(fd,"|%lld-%lld-%lld|%s|%lld|%lld-%lld-%lld",
              flip(1, 12), flip(1, 28),   // customer since
              flip(2007-age, 2007),
              stage[flip(0, 5)],          // deal stage
              flip(10000, 5000000),       // deal size
              flip(1, 12),                // last deal update
              flip(1, 28), flip(2003, 2007)
             );
    }
    fprintf(fd, "\n");
  }
  if(fd!=stdout) fclose(fd);
}

void emp_gen(const char *file, uint64 numemps)
{

  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fd = fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Employee_Dimension output file !!\n");
      exit(1);
  }

  static const char *topjob[6] =
  {
    "CEO",
    "CFO",
    "Founder",
    "Co-Founder",
    "President",
    "Investor"
  };
  static const char *highjob[6] =
  {
    "VP of Sales",
    "VP of Advertising",
    "Head of Marketing",
    "Regional Manager",
    "Director of HR",
    "Head of PR"
  };
  static const char *middlejob[6] =
  {
    "Shift Manager",
    "Assistant Director",
    "Marketing",
    "Sales",
    "Advertising",
    "Branch Manager"
  };
  static const char *lowjob[6] =
  {
    "Shelf Stocker",
    "Delivery Person",
    "Cashier",
    "Greeter",
    "Customer Service",
    "Custodian"
  };

  for(uint64 i = 1; i <= numemps; i++)
  {
    uint64 gend = flip(0, 1);
    fprintf(fd,"%lld", i);
    if(gend)
      fprintf(fd,"|%s|%s|%s", "Male", title[flip(0, 2)], mname[flip(0, 29)]);
    else
      fprintf(fd,"|%s|%s|%s", "Female", title[flip(2, 4)], fname[flip(0, 29)]);

    uint64 loc = flip(0, NUM_LOC-1);
    uint64 age = flip(14, 65);
    fprintf(fd,"|%s|%s|%lld|%lld-%lld-%lld|%lld %s|%s|%s|%s",
            midinit[flip(0, 26)],
            surname[flip(0, 49)],
            age,
            flip(1, 12), flip(1, 28), flip(2007 - age + 14, 2007), // hire date
            flip(1, 500), streets[flip(0, 19)],
            locations[loc].city,
            locations[loc].state,
            locations[loc].region
           );
     if(i <= numemps/100)            // make a chain of command
     {
       fprintf(fd,"|%s", topjob[flip(0, 5)]);
       // top employees don't report to anyone
       fprintf(fd, "|%s", nullflag);
       fprintf(fd, "|1|%lld", flip(200000, 1000000));

       fprintf(fd, "|%s", nullflag);
       fprintf(fd, "|%lld\n", flip(20, 40));
     }

     else if(i <= numemps/5)
     {
       fprintf(fd,"|%s|%lld|1|%lld",
               highjob[flip(0, 5)],
               flip(1, numemps/100),
               flip(100000, 200000));

       fprintf(fd, "|%s", nullflag);
       fprintf(fd, "|%lld\n", flip(15, 30));
     }

     else if(i <= numemps/2)
     {
       fprintf(fd,"|%s|%lld|1|%lld",
               middlejob[flip(0, 5)],
               flip(numemps/100, numemps/5),
               flip(50000, 100000));
       fprintf(fd, "|%s", nullflag);
       fprintf(fd, "|%lld\n", flip(10, 20));
     }

     else
     {
       uint64 cents = flip(0, 99);
       uint64 dollars = flip(6, 15);
       fprintf(fd,"|%s|%lld|0|%lld|%lld.%02lld|%lld\n",
               lowjob[flip(0, 5)],
               flip(numemps/5, numemps/2),
               (dollars * 200) + (cents * 2),  // annual salary assuming 200 hrs/year
               dollars, cents,
               flip(5, 15));
     }

  }
  if(fd!=stdout) fclose(fd);
}

uint64 time_gen(const char *timefile, const char *outfile)
{
  char date[15];
  char desc[100];
  static const char *dayofweek[8] =
    {
      "",
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    };
  uint64 weekDay;

  uint64  dayMonth, dayYear, dayFisMonth, dayFisYear;

  uint64  lastWeekDay, lastMonthDay;

  uint64  weekYear;
  char weekLastDate[10];
  uint64  leapYear;

  static const char *monthName[13] =
    {
      "",
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December"
    };
  static int lastDayOfMonth[13] =
    {
      0,
      31,
      -1,
      31,
      30,
      31,
      30,
      31,
      31,
      30,
      31,
      30,
      31
    };

  uint64  monthYear;

  char yearMonth[20];
  static const char *quarter[13] =
    {
      "",
      "Q1",
      "Q1",
      "Q1",
      "Q2",
      "Q2",
      "Q2",
      "Q3",
      "Q3",
      "Q3",
      "Q4",
      "Q4",
      "Q4"
    };
  char yearQuarter[20];
  char halfYear[10];
  uint64  year;
  uint64  isHoliday, isWeekday;

  char *line;
  size_t linesize=0;

  FILE *fd = fopen(timefile, "r");

  if (!fd)
  {
    fprintf(stderr, "File not found: %s\n", timefile);
    exit(2);
  }

  FILE *ofd = NULL;
  if(outfile) {
	if(!strcmp("-", outfile)) ofd=stdout;
	else ofd=fopen(outfile, "w");
    if (ofd == NULL)
    {
      fprintf(stderr, "Error : Can't create %s output file !!\n",outfile);
      exit(1);
    }
  }

  // fprintf(stderr, "Date_Dimension(date-key,date,Full Date Description, DayOfWeek, DayNumInMonth, DayNumInYear, DayNumInFisMonth, DayNumInFisYear, LastDayOfWeek, LastDayOfMonth, WeekNumInYear, MonthName, MonthNumInYear, Year-Month, Quarter, Year-Quarter, HalfYear, Year, Holiday, Weekday, Season\n");

  uint64 date_key = 1;

  while (getline(&line, &linesize, fd) != -1)
  {
    if (line[0] == '#')
      continue;

    sscanf(line, "%10s%lld%lld%lld%lld%lld%lld\n", &date, &dayMonth, &monthYear, &year,
           &weekDay, &weekYear, &leapYear);

    date[10] = '\0';

    if (dayMonth == 1 && monthYear == 1)
      dayYear = 1; // reset

	if(outfile) {
      sprintf(desc, "%s %lld, %lld", monthName[monthYear], dayMonth, year);

      fprintf(ofd,"%lld", date_key);
      fprintf(ofd,"|%10s",date);
      fprintf(ofd,"|%s",desc);
      fprintf(ofd,"|%s",dayofweek[weekDay]);
      fprintf(ofd,"|%lld",dayMonth);
      fprintf(ofd,"|%lld",dayYear);
      fprintf(ofd,"|%lld",dayMonth);
      fprintf(ofd,"|%lld",dayYear);   // fiscal
      fprintf(ofd,"|%lld",(weekDay == 7) ? 1:0); // last day of week
      fprintf(ofd,"|%lld",
             ((dayMonth == lastDayOfMonth[monthYear]) ||
             (monthYear == 2 /* Feb */ && ((!leapYear && dayMonth == 28) ||
                                           (leapYear && dayMonth == 29)))) ? 1:0);
      fprintf(ofd,"|%lld",weekYear);
      fprintf(ofd,"|%s",monthName[monthYear]);
      fprintf(ofd,"|%lld", monthYear);
      fprintf(ofd,"|%lld-%lld", year, monthYear);
      fprintf(ofd,"|%lld", (monthYear < 4 ? 1:
                     monthYear < 7 ? 2:
                     monthYear < 10 ? 3: 4));
      fprintf(ofd,"|%lld-%s", year, quarter[monthYear]);
      fprintf(ofd,"|%lld", monthYear <=6 ? 1:2);
      fprintf(ofd,"|%lld", year);
      fprintf(ofd,"|%s", ((monthYear == 1 && dayYear == 1) ||
                     (monthYear == 7 && dayMonth == 4) ||
                     (monthYear == 10 && dayMonth == 30) ||
                     (monthYear == 12 && dayMonth == 25)) ? "Holiday":"NonHoliday");
      fprintf(ofd,"|%s", (weekDay == 1 || weekDay == 7) ? "Weekend":"Weekday");
      fprintf(ofd,"|%s", ((monthYear < 2)? "ValentinesDay":
                     (monthYear < 4) ? "Easter":
                     (monthYear < 8)? "July4th":
                     (monthYear < 10) ? "Thanksgiving":
                     "Christmas"));
      fprintf(ofd,"\n");
	  }

    dayYear++;        // increment day of year
    date_key++;
  }
  fclose(fd);
  if(outfile) if(ofd!=stdout) fclose(ofd);
  if (line)
    free(line);

  return date_key-1;
}

void warehouse_gen(const char *file, uint64 numwarehouses)
{

  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fd=fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Warehouse_Dimension output file !!\n");
      exit(1);
  }

  static const char *warehousename[] =
  {
	  "Warehouse 1",
	  "Warehouse 2",
	  "Warehouse 3",
	  "Warehouse 4",
	  "Warehouse 5"
  };

  for(uint64 i = 1; i <= numwarehouses; i++)
  {
    uint64 loc = flip(0, NUM_LOC-1);
    fprintf(fd,"%lld|%s|%lld %s|%s|%s|%s\n",
             i,
             warehousename[flip(0, 4)],
             flip(1, 500), streets[flip(0, 19)],
             locations[loc].city,
             locations[loc].state,
             locations[loc].region
            );
  }
  if(fd!=stdout) fclose(fd);

}


void shipping_gen(const char *file, uint64 numshipping)
{

  FILE *fd = fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Shipping_Dimension output file !!\n");
      exit(1);
  }

  static const char *shiptype[] =
  {
	"REGULAR",
	"EXPRESS",
	"NEXT DAY",
	"OVERNIGHT",
	"TWO DAY",
	"LIBRARY"
  };

  static const char *shipmode[] =
  {
	  "AIR",
	  "SURFACE",
	  "SEA",
	  "BIKE",
	  "HAND CARRY",
	  "MESSENGER",
	  "COURIER"
  };

  static const char *shipcarrier[] =
  {
	"UPS",
	"FEDEX",
	"AIRBORNE",
	"USPS",
	"DHL",
	"TBS",
	"ZHOU",
	"ZOUROS",
	"MSC",
	"LATVIAN",
	"ALLIANCE",
	"ORIENTAL",
	"BARIAN",
	"BOXBUNDLES",
	"GREAT EASTERN",
	"DIAMOND",
	"RUPEKSA",
	"GERMA",
	"HARMSTORF",
	"PRIVATECARRIER"
  };


  for(uint64 i = 1; i <= numshipping; i++)
  {
    fprintf(fd,"%lld|%s|%s|%s\n",
             i,
             shiptype[flip(0,5)],
             shipmode[flip(0,6)],
             shipcarrier[flip(0,19)]
            );
  }
  if(fd!=stdout) fclose(fd);

}

void onlinepage_gen(const char *file, uint64 numonlinepages)
{

  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fd=fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Onlinepage_Dimension output file !!\n");
      exit(1);
  }

  static const char *op_description[] =
  {
	"Online Page Description #1",
	"Online Page Description #2",
	"Online Page Description #3",
	"Online Page Description #4",
	"Online Page Description #5",
	"Online Page Description #6",
	"Online Page Description #7",
	"Online Page Description #8"
  };

  static const char *op_type[] =
  {
	  "bi-annual",
	  "quarterly",
	  "monthly"
  };


  for(uint64 i = 1; i <= numonlinepages; i++)
  {
    fprintf(fd,"%lld",i);

    uint64 month = flip(1,12);
	uint64 day = flip(1, 14);
	uint64 year = flip(2003, 2007);

    fprintf(fd,"|%lld-%lld-%lld", month, day, year);
    fprintf(fd,"|%lld-%lld-%lld", month, day + flip(1, 14), year);

    fprintf(fd,"|%lld|%s|%s\n",
             flip(1,40),
             op_description[flip(0,7)],
             op_type[flip(0,2)]
            );

  }
  if(fd!=stdout) fclose(fd);

}

void callcenter_gen(const char *file, uint64 numcallcenters)
{

  FILE *fd = NULL;
  if(!strcmp("-", file)) fd=stdout;
  else fd=fopen(file, "w");
  if (fd == NULL)
  {
      fprintf(stderr, "Error : Can't create Callcenter_Dimension output file !!\n");
      exit(1);
  }

  static const char *cc_name[] =
  {
	"New England",
	"NY Metro",
	"Mid Atlantic",
	"Southeastern",
	"North Midwest",
	"Central Midwest",
	"South Midwest",
	"Pacific Northwest",
	"California",
	"Southwest",
	"Hawaii/Alaska",
	"Other"
  };

  static const char *cc_class[] =
  {
	 "small",
	 "medium",
	 "large"
  };

  static const char *cc_hours[] =
  {
	 "8AM-4PM",
	 "8AM-12AM",
	 "8AM-8AM"
  };

  static const char *cc_manager[] =
  {
	 "Bob Belcher",
	 "Felipe Perkins",
	 "Mark Hightower",
	 "Larry Mccray"
  };

  uint64 month,day,year;

  for(uint64 i = 1; i <= numcallcenters; i++)
  {
	uint64 loc = flip(0, NUM_LOC-1);

    fprintf(fd,"%lld",i);

    month = flip(1,12);
	day = flip(1, 14);
	year = flip(2003, 2007);

    fprintf(fd,"|%lld-%lld-%lld", month, day + flip(1, 14), year);
    fprintf(fd,"|%lld-%lld-%lld", month, day, year);

    fprintf(fd,"|%s|%s|%lld|%s|%s|%lld %s|%s|%s|%s\n",
             cc_name[flip(0,11)],
             cc_class[flip(0,2)],
             flip(1,30),
             cc_hours[flip(0,2)],
             cc_manager[flip(0,3)],
             flip(1, 500), streets[flip(0, 19)],
             locations[loc].city,
             locations[loc].state,
             locations[loc].region
			);

  }
  if(fd!=stdout) fclose(fd);

}

void make_loadnull()
{
  int len = strlen(nullflag);
  int index = 0;

  for(int i = 0; i < len; i++)
  {
    loadnull[index++] = nullflag[i];
    if(nullflag[i] == '\\')
    {
      loadnull[index++] = '\\';
      if(i == len-1)
      {
        fprintf(stderr, "The null value cannot end in a '\\'\n");
        exit(1);
      }
    }
  }
}

void tableprint(FILE *fd, const char *inputfile, const char *table)
{
  char buf[1024];
  sprintf(buf, "\\set input_file '\\'':t_pwd'/");
  sprintf(&buf[strlen(buf)], "%s.tbl\\''\n", inputfile);
  sprintf(&buf[strlen(buf)], "COPY %s FROM :input_file DELIMITER '|' ", table);

  // if nullflag contains a '\', we need to add extra dashes
  // to get the correct value into the load script
  make_loadnull();
  sprintf(&buf[strlen(buf)], "NULL '%s' DIRECT;\n\n", loadnull);

  fprintf(fd, "%s", buf);
}

void gen_load_script(const char *filename,uint64 numfiles,int pwd)
{
  
  FILE *fd = NULL;
  if(!strcmp("-", filename)) fd=stdout;
  else fd=fopen(filename, "w");
  if(fd == NULL)
  {
    fprintf(stderr, "Cannot create data load file.\n");
    exit(1);
  }
  
  if(pwd==1)
  {
	const char *firstline = "\\set t_pwd `pwd`\n\n";
    fprintf(fd, "%s", firstline);
  }
  else
  {
	const char *firstline = "\\set t_pwd :datadirectory\n\n";
    fprintf(fd, "%s", firstline);
  }

  const char *tables[15] =
  {
    "Date_Dimension",
    "Product_Dimension",
    "store.Store_Dimension",
    "Promotion_Dimension",
    "Vendor_Dimension",
    "Customer_Dimension",
    "Employee_Dimension",
    "Warehouse_Dimension",
    "Shipping_Dimension",
    "online_sales.Online_Page_Dimension",
    "online_sales.Call_Center_Dimension",
    "store.Store_Sales_Fact",
    "store.Store_Orders_Fact",
    "online_sales.Online_Sales_Fact",
    "Inventory_Fact"
  };

  const char *inputfile[15] =
  {
    "Date_Dimension",
    "Product_Dimension",
    "Store_Dimension",
    "Promotion_Dimension",
    "Vendor_Dimension",
    "Customer_Dimension",
    "Employee_Dimension",
    "Warehouse_Dimension",
    "Shipping_Dimension",
    "Online_Page_Dimension",
    "Call_Center_Dimension",
    "Store_Sales_Fact",
    "Store_Orders_Fact",
    "Online_Sales_Fact",
    "Inventory_Fact"
  };


  for(int i = 0; i < 15; i++)
  {
    if(i < 11 || numfiles == 1)
      tableprint(fd, inputfile[i], tables[i]);
    else // we've got a fact table that is split into multiple files
    {
      for(int j = 1; j <= numfiles; j++)
      {
        char fact[32];
        strcpy(fact, inputfile[i]);
        sprintf(&fact[strlen(fact)], "_%03d", j);
        tableprint(fd, fact, tables[i]);
      }
    }
  }
}


