# Refactoring KPI_Pythhon  
## Environmernt variables (under development)
There are several environment .env files:  
1. `.env.base` Base environment file:  
    Includes two environment variables:  
        - ENTORNO: `"dev"` for delepment context and `"pro"` for production  
        - INTEGRATION_CUST: three letters that correspond to a specific customer
2. `.env.anedev` Specific environment corresponding an integration customer , Includes the rest of the envirom¡nment variables for this customer  
It will be one `.env` for each integration customer `.ane` and for a context `pro` = `.emv.anepro`
