const Binance = require('./binance')
const fs = require('fs');
const path = require('path');
const ccxt = require('ccxt');


class UDFError extends Error { }
class SymbolNotFound extends UDFError { }
class InvalidResolution extends UDFError { }

class UDF {
    constructor() {
        this.binance = new Binance()
        this.supportedResolutions = ['1', '3', '5', '15', '30', '60', '120', '240', '360', '480', '720', '1D', '3D', '1W', '1M']

        setInterval(() => { this.loadSymbols() }, 30000)
        this.loadSymbols()
    }

    loadSymbols() {
        function pricescale(symbol) {
            for (let filter of symbol.filters) {
                if (filter.filterType == 'PRICE_FILTER') {
                    return Math.round(1 / parseFloat(filter.tickSize))
                }
            }
            return 1
        }

        const promise = this.binance.exchangeInfo().catch(err => {
            console.error(err)
            setTimeout(() => {
                this.loadSymbols()
            }, 1000)
        })
        this.symbols = promise.then(info => {
            return info.symbols.map(symbol => {
                return {
                    symbol: symbol.symbol,
                    ticker: symbol.symbol,
                    name: symbol.symbol,
                    full_name: symbol.symbol,
                    description: `${symbol.baseAsset} / ${symbol.quoteAsset}`,
                    exchange: 'BINANCE',
                    listed_exchange: 'BINANCE',
                    type: 'crypto',
                    currency_code: symbol.quoteAsset,
                    session: '24x7',
                    timezone: 'UTC',
                    minmovement: 1,
                    minmov: 1,
                    minmovement2: 0,
                    minmov2: 0,
                    pricescale: pricescale(symbol),
                    supported_resolutions: this.supportedResolutions,
                    has_intraday: true,
                    has_daily: true,
                    has_weekly_and_monthly: true,
                    data_status: 'streaming'
                }
            })
        })
        this.allSymbols = promise.then(info => {
            let set = new Set()
            for (const symbol of info.symbols) {
                set.add(symbol.symbol)
            }
            return set
        })
    }

    async checkSymbol(symbol) {
        const symbols = await this.allSymbols
        return symbols.has(symbol)
    }

    /**
     * Convert items to response-as-a-table format.
     * @param {array} items - Items to convert.
     * @returns {object} Response-as-a-table formatted items.
     */
    asTable(items) {
        let result = {}
        for (const item of items) {
            for (const key in item) {
                if (!result[key]) {
                    result[key] = []
                }
                result[key].push(item[key])
            }
        }
        for (const key in result) {
            const values = [...new Set(result[key])]
            if (values.length === 1) {
                result[key] = values[0]
            }
        }
        return result
    }

    /**
     * Data feed configuration data.
     */
    async config() {
        return {
            exchanges: [
                {
                    value: 'BINANCE',
                    name: 'Binance',
                    desc: 'Binance Exchange'
                }
            ],
            symbols_types: [
                {
                    value: 'crypto',
                    name: 'Cryptocurrency'
                }
            ],
            supported_resolutions: this.supportedResolutions,
            supports_search: true,
            supports_group_request: false,
            supports_marks: false,
            supports_timescale_marks: false,
            supports_time: true
        }
    }

    /**
     * Symbols.
     * @returns {object} Response-as-a-table formatted symbols.
     */
    async symbolInfo() {
        const symbols = await this.symbols
        return this.asTable(symbols)
    }

    /**
     * Symbol resolve.
     * @param {string} symbol Symbol name or ticker.
     * @returns {object} Symbol.
     */
    async symbol(symbol) {
        const symbols = await this.symbols

        const comps = symbol.split(':')
        const s = (comps.length > 1 ? comps[1] : symbol).toUpperCase()

        for (const symbol of symbols) {
            if (symbol.symbol === s) {
                return symbol
            }
        }

        throw new SymbolNotFound()
    }

    /**
     * Symbol search.
     * @param {string} query Text typed by the user in the Symbol Search edit box.
     * @param {string} type One of the symbol types supported by back-end.
     * @param {string} exchange One of the exchanges supported by back-end.
     * @param {number} limit The maximum number of symbols in a response.
     * @returns {array} Array of symbols.
     */
    async search(query, type, exchange, limit) {
        let symbols = await this.symbols
        if (type) {
            symbols = symbols.filter(s => s.type === type)
        }
        if (exchange) {
            symbols = symbols.filter(s => s.exchange === exchange)
        }

        query = query.toUpperCase()
        symbols = symbols.filter(s => s.symbol.indexOf(query) >= 0)

        if (limit) {
            symbols = symbols.slice(0, limit)
        }
        return symbols.map(s => ({
            symbol: s.symbol,
            full_name: s.full_name,
            description: s.description,
            exchange: s.exchange,
            ticker: s.ticker,
            type: s.type
        }))
    }


    /* borei modifications */

    RESOLUTIONS_INTERVALS_MAP = {
        '1': '1m',
        '3': '3m',
        '5': '5m',
        '15': '15m',
        '30': '30m',
        '60': '1h',
        '120': '2h',
        '240': '4h',
        '360': '6h',
        '480': '8h',
        '720': '12h',
        'D': '1d',
        '1D': '1d',
        '3D': '3d',
        'W': '1w',
        '1W': '1w',
        'M': '1M',
        '1M': '1M',
    }

    createCacheFolder() {
        fs.mkdirSync('tickers', { recursive: true });
    }

    getCacheFilePath(symbol, interval) {
        return `tickers/${symbol}_${interval}.json`;
    }

    loadCachedData(cacheFile) {
        if (fs.existsSync(cacheFile)) {
            return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
        }
        return [];
    }

    saveDataToCache(cacheFile, data) {
        fs.writeFileSync(cacheFile, JSON.stringify(data), 'utf8');
        console.log(`Data saved to cache file ${cacheFile}`);
        if (fs.existsSync(cacheFile)) {
            console.log(`Le fichier ${cacheFile} a bien été créé.`);
        } else {
            console.log(`Erreur : le fichier ${cacheFile} n'a pas été créé.`);
        }
    }

    findGaps(cachedData, from, to, msInterval, maxCandles = 999) {
        const gaps = [];

        // Si le cache est vide, retourne un gap pour l'intervalle entier demandé
        if (cachedData.length === 0) {
            console.log("Le fichier cache n'existe pas."); // Condition 1
            return [{ from, to }];
        }

        const addGaps = (gapStart, gapEnd) => {
            // Remplit les gaps avec des intervalles de temps allant de gapStart à gapEnd
            while (gapStart <= gapEnd) {
                const newGap = {
                    from: gapStart,
                    to: Math.min(gapStart + msInterval * (maxCandles - 1), gapEnd),
                };

                // Vérifie si le gap est valide et non vide
                if (newGap.from <= newGap.to) {
                    gaps.push(newGap);
                }

                gapStart += msInterval * maxCandles;
            }
        };


        // Vérifie si un gap est nécessaire avant la première donnée dans le cache
        if (cachedData[0][0] > from) {
            console.log("Le fichier cache existe mais ne contient que la fin des données de l'intervalle de la requête. from: " + this.formatDate(from) + ", date de début du cache: " + this.formatDate(cachedData[0][0])); // Condition 3
            addGaps(from, Math.min(cachedData[0][0] - msInterval, to));
        }

        // Parcourt les données du cache et vérifie s'il y a des gaps entre les données
        for (let i = 0; i < cachedData.length - 1; i++) {
            const prevTs = cachedData[i][0];
            const nextTs = cachedData[i + 1][0];
            // Si la différence entre les deux timestamps adjacents est supérieure à msInterval, il y a un gap
            if (nextTs - prevTs > msInterval) {
                console.log("Le fichier cache existe mais il est fragmenté sur les plages visibles demandés. Gap manquant entre " + this.formatDate(prevTs + msInterval) + " et " + this.formatDate(nextTs - msInterval)); // Condition 3
                addGaps(prevTs + msInterval, nextTs - msInterval);
            }
        }

        // Vérifie si un gap est nécessaire après la dernière donnée dans le cache
        if (cachedData[cachedData.length - 1][0] < to - msInterval) {
            console.log("Le fichier cache existe mais ne contient que le début des données de l'intervalle de la requête. To: " + this.formatDate(to) + ", date de fin du cache: " + this.formatDate(cachedData[cachedData.length - 1][0])); // Condition 3
            addGaps(cachedData[cachedData.length - 1][0] + msInterval, to - msInterval);
        }

        // Si aucun gap n'est trouvé, cela signifie que le cache contient toutes les données demandées
        if (gaps.length === 0) {
            console.log("Le fichier cache existe et contient toutes les données demandées."); // Condition 4
        }
        // Affiche le contenu des gaps avant de les retourner, en utilisant formatDate pour afficher les dates
        console.log("Gaps trouvés :");
        for (const gap of gaps) {
            console.log(
                "Gap du " +
                this.formatDate(gap.from) +
                " au " +
                this.formatDate(gap.to)
            );
        }
        return gaps;
    }

    formatDate(timestamp) {
        const date = new Date(timestamp);
        const options = {
            year: 'numeric',
            month: 'long',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
        };
        return date.toLocaleString('fr-FR', options);
    }

    formatTimestamp(timestamp) {
        const date = new Date(timestamp);

        const day = date.getDate().toString().padStart(2, '0');
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const year = date.getFullYear();

        const hours = date.getHours().toString().padStart(2, '0');
        const minutes = date.getMinutes().toString().padStart(2, '0');

        return `${day} ${month} ${year} à ${hours}h${minutes}`;
    }

    async fetchMissingData(binance, symbol, interval, cachedData, gaps, msInterval) {
        const newData = [];

        for (const gap of gaps) {
            let gapStart = gap.from;
            let gapEnd = gap.to;

            while (gapStart <= gapEnd) {
                const missingData = await this.binance.klines(symbol, interval, gapStart, Math.min(gapStart + msInterval * 999, gapEnd));

                if (missingData.length === 0) {
                    break; // Sort de la boucle while si missingData.length est égal à 0
                }

                for (const data of missingData) {
                    const existsInCache = cachedData.some(cachedDataPoint => cachedDataPoint[0] === data[0]);
                    const existsInNewData = newData.some(newDataPoint => newDataPoint[0] === data[0]);


                    if (!existsInCache && !existsInNewData) {
                        const formattedData = data.map((value, index) => index === 0 ? this.formatTimestamp(value) : value);
                        console.log("Added missing data:", formattedData);
                        newData.push(data);
                    } else {
                        const formattedData = data.map((value, index) => index === 0 ? this.formatTimestamp(value) : value);
                        console.log("Data already exists:", formattedData);
                    }
                }

                gapStart += msInterval * missingData.length;
            }
        }

        // Merge new data with cached data and sort by timestamp
        const updatedData = [...cachedData, ...newData];
        updatedData.sort((a, b) => a[0] - b[0]);

        // Remove duplicates
        const deduplicatedData = [];
        for (let i = 0; i < updatedData.length; i++) {
            if (i === 0 || updatedData[i][0] !== updatedData[i - 1][0]) {
                deduplicatedData.push(updatedData[i]);
            }
        }

        return deduplicatedData;
    }



    filterDataByRange(cachedData, from, to) {
        return cachedData.filter(x => from <= x[0] && x[0] < to);
    }

    resolutionToMilliseconds(resolution) {
        if (!resolution) {
            console.log("resolution inconnu :" + resolution);
            throw new InvalidResolution();
        }

        const interval = resolution
        //const interval = this.RESOLUTIONS_INTERVALS_MAP[resolution];
        const unit = interval.slice(-1);
        const value = parseInt(interval.slice(0, -1));

        let milliseconds;
        switch (unit) {
            case 'm':
                milliseconds = value * 60 * 1000;
                break;
            case 'h':
                milliseconds = value * 60 * 60 * 1000;
                break;
            case 'd':
                milliseconds = value * 24 * 60 * 60 * 1000;
                break;
            case 'w':
                milliseconds = value * 7 * 24 * 60 * 60 * 1000;
                break;
            case 'M':
                milliseconds = value * 30 * 24 * 60 * 60 * 1000; // Approximation
                break;
            default:
                throw new InvalidResolution();
        }

        return milliseconds;
    }


    async history(symbol, from, to, resolution) {
        const hasSymbol = await this.checkSymbol(symbol)
        if (!hasSymbol) {
            throw new SymbolNotFound()
        }

        const interval = this.RESOLUTIONS_INTERVALS_MAP[resolution];
        if (!interval) {
            throw new InvalidResolution()
        }
        //const interval = resolution;
        this.createCacheFolder();

        const cacheFile = this.getCacheFilePath(symbol, interval);
        const msInterval = this.resolutionToMilliseconds(interval); // Utilisez cette fonction pour convertir l'intervalle en millisecondes
        let cachedData = this.loadCachedData(cacheFile);

        if (cachedData.length === 0) {
            console.log("Le fichier cache n'existe pas."); // Condition 1
            const binance = new ccxt.binance({ 'enableRateLimit': true });
            cachedData = await this.fetchMissingData(binance, symbol, interval, cachedData, [{ from: from * 1000, to: to * 1000 }], msInterval);
            this.saveDataToCache(cacheFile, cachedData);
        } else {
            console.log("Le fichier cache existe."); // Conditions 3 et 4
            const gaps = this.findGaps(cachedData, from * 1000, to * 1000, msInterval, 999);
            if (gaps.length > 0) {
                const binance = new ccxt.binance({ 'enableRateLimit': true });
                cachedData = await this.fetchMissingData(binance, symbol, interval, cachedData, gaps, msInterval);
                this.saveDataToCache(cacheFile, cachedData);
            }
        }

        const filteredData = this.filterDataByRange(cachedData, from * 1000, to * 1000);

        return {
            s: 'ok',
            t: filteredData.map(b => Math.floor(b[0] / 1000)),
            c: filteredData.map(b => parseFloat(b[4])),
            o: filteredData.map(b => parseFloat(b[1])),
            h: filteredData.map(b => parseFloat(b[2])),
            l: filteredData.map(b => parseFloat(b[3])),
            v: filteredData.map(b => parseFloat(b[5]))
        };
    }


    /* borei modifications fin */
    /**
     * Bars.
     * @param {string} symbol - Symbol name or ticker.
     * @param {number} from - Unix timestamp (UTC) of leftmost required bar.
     * @param {number} to - Unix timestamp (UTC) of rightmost required bar.
     * @param {string} resolution
     */
    /*     async history(symbol, from, to, resolution) {
            const hasSymbol = await this.checkSymbol(symbol)
            if (!hasSymbol) {
                throw new SymbolNotFound()
            }
    
            const RESOLUTIONS_INTERVALS_MAP = {
                '1': '1m',
                '3': '3m',
                '5': '5m',
                '15': '15m',
                '30': '30m',
                '60': '1h',
                '120': '2h',
                '240': '4h',
                '360': '6h',
                '480': '8h',
                '720': '12h',
                'D': '1d',
                '1D': '1d',
                '3D': '3d',
                'W': '1w',
                '1W': '1w',
                'M': '1M',
                '1M': '1M',
            }
    
            const interval = RESOLUTIONS_INTERVALS_MAP[resolution]
            if (!interval) {
                throw new InvalidResolution()
            }
    
            let totalKlines = []
    
            from *= 1000
            to *= 1000
    
            while (true) {
                const klines = await this.binance.klines(symbol, interval, from, to, 500)
                totalKlines = totalKlines.concat(klines)
                if (klines.length == 500) {
                    from = klines[klines.length - 1][0] + 1
                } else {
                    if (totalKlines.length === 0) {
                        return { s: 'no_data' }
                    } else {
                        return {
                            s: 'ok',
                            t: totalKlines.map(b => Math.floor(b[0] / 1000)),
                            c: totalKlines.map(b => parseFloat(b[4])),
                            o: totalKlines.map(b => parseFloat(b[1])),
                            h: totalKlines.map(b => parseFloat(b[2])),
                            l: totalKlines.map(b => parseFloat(b[3])),
                            v: totalKlines.map(b => parseFloat(b[5]))
                        }
                    }
                }
            }
        } */
}

UDF.Error = UDFError
UDF.SymbolNotFound = SymbolNotFound
UDF.InvalidResolution = InvalidResolution

module.exports = UDF
