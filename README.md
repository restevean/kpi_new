# Refactoring KPI_Pythhon  
## Environmernt variables (under development)
There are several environment .env files:  
1. `.env.base` Base environment file:  
    Includes two environment variables:  
        - ENTORNO: `"dev"` for delepment context and `"prod"` for production  
        - INTEGRATION_CUST: 
2. `.env.001` Specific environment corresponding an integration customer, Includes the rest of the envirom¡nment variables for this customer  
It will be one `.env.nnn` for each integration customer