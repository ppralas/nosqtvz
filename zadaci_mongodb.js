/*
	KOMENTAR:Nakon downloada Monga i prije pokretanja koda potrebno je napraviti sljedeće:
	 1. Napraviti novu konekciju u MongoDBCompassu : "mongodb+srv://patrik:patrik@forestfires.hdln0jn.mongodb.net/test".
	 2. Kreirati novu bazu pritiskom na gumb + , te kreirati novu kolekciju i nazvati je po želji.
	 3. Importati podatke iz .csv file-a u kolekciju, kategoričke vrijednosti postaviti kao String, a kontinuirane kao Number. 
	 4. Pokrenuti mongodb daemon (mongod).
	 5. Pokrenuti mongos (mongo shell) i krenuti sa pokretanjem skripti. 
*/

/*
	Zadatak 1.
	
	Sve nedostajuće vrijednosti kontinuirane varijable zamijeniti sa -1, a kategoričke sa „empty“.
*/

print("1.Zadatak: In progress...");
//Dohvaćanje baze
var BAZA_PROJEKT = db.getSiblingDB("projekt");

//Dohvaćanje baze i kolekcije
var collection_forestfires = BAZA_PROJEKT.getCollection("forestfires");

//Kontinuirane varijable (Number)
var continious_variables_array = ["X","Y","FFMC","DMC","DC","ISI","temp","RH","wind","rain","area"];

//Kategoričke varijable (String)
var category_variables_array = ["month","day"];


//ForEach petlja iterira kroz kategoričke varijable i prazne vrijednosti zamjenjuje sa "empty"


category_variables_array.forEach(category_variable => {
	collection_forestfires.updateMany(
		{ [category_variable]: null },
		{ $set: { [category_variable]: "empty" } }
	);
});

//ForEach petlja iterira kroz kontinuirane varijable i prazne vrijednosti zamjenjuje sa -1

continious_variables_array.forEach(continious_variable => {
	collection_forestfires.updateMany(
		{ [continious_variable]: null },
		{ $set: { [continious_variable]: -1 } }
	);
});

print("1.Zadatak: Finished");

//==========================================================================================================================


/*
	Zadatak 2.
	
	Za svaku kontinuiranu vrijednost izračunati srednju vrijednost, standardnu devijaciju i kreirati 
	novi dokument oblika sa vrijednostima, dokument nazvati:  statistika_ {ime vašeg data seta}. 
	U izračun se uzimaju samo nomissing  vrijednosti.
*/
print("2.Zadatak: In progress...");

//Dohvaćanje kolekcije statistika_forestfires
var collection_statistics = BAZA_PROJEKT.getCollection("statistika_forestfires");

// Provjera da li kolekcija postoji (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.
/*
	KOMENTAR: Može se koristiti i if else blok, isti je efekt.
*/
(collection_statistics != null) ? collection_statistics.remove({}) : BAZA_PROJEKT
	.createCollection("statistika_forestfires");

//Prolazak kroz kontinuirane varijable
continious_variables_array.forEach(continious_var_array => {
	//Spremanje srednje vrijednosti, standardne deviacije i broj non missisg elemenata u polje, te dodavanje polja Name ($addFields)
	var result = collection_forestfires
		.aggregate([
			{
				$group: {
					_id: new ObjectId(),
					srednjaVrijednost: { $avg: '$' + continious_var_array },
					StandardnaDevijacija: { $stdDevPop: '$' + continious_var_array },
					NoMissingElement: {
						$sum: { $sum: 1 },
					},
				},
			},
			{$addFields: { Name: continious_var_array }},
		]).toArray();

	collection_statistics.insert(result);
})


print("2.Zadatak: Finished");

//==========================================================================================================================

/*
	Zadatak 3.
	Za svaku kategoričku vrijednost izračunati frekvencije pojavnosti 
	po obilježjima varijabli i kreirati novi dokument koristeći nizove, 
	dokument nazvati: frekvencija_ {ime vašeg data seta}. 
	Frekvencije računati koristeći $inc modifikator.
*/
print("3.Zadatak: In progress...");
//Dohvaćanje kolekcije frekvencija_forestfires
var collection_frequency = BAZA_PROJEKT.getCollection("frekvencija_forestfires");
// Provjera da li kolekcija postoji (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.
(collection_frequency != null) ? collection_frequency.remove({}) : BAZA_PROJEKT
	.createCollection("frekvencija__forestfires");
//array pojavnosti
var incidence_array = [];

category_variables_array.forEach(category_variable => {
	collection_frequency.insert(
		{
			varijabla: category_variable,
			pojavnost: []
		}
	);
	// Izvlačenje jedinstvenih vrijednosti iz varijable
	incidence_array = collection_forestfires.distinct(category_variable)
	incidence_array.forEach(incidence_variable => {
		//spremanje jedistvene vrijednosti (atribut)
		collection_frequency.update(
			{ varijabla: category_variable },
			{
				$push: {
					pojavnost: { atribut: incidence_variable, broj: NumberLong(0) }
				}
			});
	})
	//Inkrementirati pojavnost atributa 
	collection_forestfires.find().forEach(column => {
		for (var icount in column) {
			if (icount == category_variable) {
				collection_frequency.update(
					{ varijabla: icount, "pojavnost.atribut": column[icount] },
					{ $inc: { "pojavnost.$.broj": 1 } });
			}
		}
	});
})
print("3.Zadatak: Finished");

//==========================================================================================================================

/*
	Zadatak 4.
	
	Iz osnovnog dokumenta kreirati dva nova dokumenta sa kontinuiranim vrijednostima u kojoj 
	će u prvom dokumentu biti sadržani svi elementi <= srednje vrijednosti, a u drugom dokumentu 
	biti sadržani svi elementi > srednje vrijednosti, dokument nazvati:  
	statistika1_ {ime vašeg data seta} i  statistika2_ {ime vašeg data seta}
*/
print("4.Zadatak: In progress...");
//Dohvaćanje kolekcije statistika1_forestfires
var collection_small_statistika = BAZA_PROJEKT.getCollection("statistika1_forestfires");

// Provjera da li postoji kolekcija collection_small_statistika (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.
(collection_small_statistika != null) ? collection_small_statistika.remove({}) : BAZA_PROJEKT
	.createCollection("statistika1_forestfires");

//Dohvaćanje kolekcije statistika2_forestfires
var collection_big_statistika = BAZA_PROJEKT.getCollection("statistika2_forestfires");

// Provjera da li kolekcija coll_big_statistika (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.
(collection_big_statistika != null) ? collection_big_statistika.remove({}) : BAZA_PROJEKT
	.createCollection("statistika2_forestfires");

continious_variables_array.forEach(continious_variable => {

	var vrijednosti_manje = [];
	var vrijednosti_vece = [];

	var main_stat = collection_statistics.findOne(
		{ Name: continious_variable }
	);

	var srednja_vrijednost = main_stat.srednjaVrijednost;

	collection_forestfires.find().forEach(column => {
		for (var item in column) {
			//Kondicionalni (ternarni) operator, operator unutar operatora
			(item == continious_variable && column[item] <= srednja_vrijednost) ? vrijednosti_manje.push(column[item]) :
			((item == continious_variable && column[item] > srednja_vrijednost) ? vrijednosti_vece.push(column[item]) : null);
		}
	});

	collection_small_statistika
		.insert({ varijabla: continious_variable, vrijednosti_manje: vrijednosti_manje });

	collection_big_statistika
		.insert({ varijabla: continious_variable, vrijednosti_vece: vrijednosti_vece }
		);
})

print("4.Zadatak: Finished");

//==========================================================================================================================

/*
	Zadatak 5.
	
   Osnovni dokument kopirati u novi te embedati vrijednosti iz tablice 3 
   za svaku kategoričku vrijednost, :  emb_ {ime vašeg data seta} 
*/
print("5.Zadatak: In progress...");
// Provjera da li postoji kolekcija collection_embbeded (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.

//Dohvaćanje kolekcije emb_forestfires
var collection_embbeded = BAZA_PROJEKT.getCollection("emb_forestfires");
// Provjera da li postoji kolekcija collection_embbeded (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.
(collection_embbeded != null) ? collection_embbeded.remove({}) : BAZA_PROJEKT
	.createCollection("emb_forestfires");

var main_item_array_one = collection_forestfires.find().toArray();
var frequency_item_array = collection_frequency.find().toArray();

main_item_array_one.forEach(main_item => {
	collection_embbeded.updateOne(
		{ _id: main_item['_id'] },
		{
			$push: { embedded_elements: { $each: frequency_item_array } },
			$set: main_item
		},
		{ upsert: true }
	)
})

print("5.Zadatak: Finished");


//==========================================================================================================================


/*
	Zadatak 6.
	
	Osnovni  dokument  kopirati u novi te embedati vrijednosti iz tablice 2 za svaku kontinuiranu  vrijednost kao niz :  emb2_ {ime vašeg data seta} 
*/
print("6.Zadatak: In progress...");
//Dohvaćanje kolekcije emb2_forestfires
var collection_embbeded2 = BAZA_PROJEKT.getCollection("emb2_forestfires");
// Provjera da li postoji kolekcija collection_embbeded2 (ternarni operator), ako postoji obriši ju,
// ako ne postoji kreiraj ju.
(collection_embbeded2 != null) ? collection_embbeded2.remove({}) : BAZA_PROJEKT
	.createCollection("emb2_forestfires");

var main_item_array_two = collection_forestfires.find().toArray();
var statistics_item_array = collection_statistics.find().toArray();

main_item_array_two.forEach(main_item => {
	collection_embbeded2.updateOne(
		{ _id: main_item['_id'] },
		{
			$push: { embedded_elements: { $each: statistics_item_array } },
			$set: main_item
		},
		{ upsert: true }
	)
})

print("6.Zadatak: Finished");

//==========================================================================================================================

/*
	Zadatak 7.

	Iz tablice emb2 izvući sve one srednje vrijednosti  iz  nizova čija je standardna devijacija 10% > srednje vrijednosti koristeći $set modifikator
*/
print("7.Zadatak: In progress...");
var general_item_array = collection_embbeded2.find().toArray();

general_item_array.forEach(general_item => {

	general_item.embedded_elements.forEach(embedded_element => {
		var deviation = Math.round(((embedded_element.StandardnaDevijacija / embedded_element.srednjaVrijednost) - 1) * 100);
		if (deviation > 10) {
			collection_embbeded2.updateOne(
				{ _id: embedded_element['_id'] },
				{
					$set:
					{
						"srednjaVrijednost": embedded_element.srednjaVrijednost,
						"StandardnaDevijacija": embedded_element.StandardnaDevijacija
					}
				},
				{ upsert: true }
			)
		}
	})

});

print("7.Zadatak: Finished");

//==========================================================================================================================

/*
	Zadatak 8.

	Kreirati složeni indeks na originalnoj tablici i osmisliti upit koji je kompatibilan sa indeksom.
*/
print("8.Zadatak: In progress...");
collection_forestfires.dropIndexes();

collection_forestfires.createIndex(
	{ 'Run1 (ms)': 1, 'Run2 (ms)': 1, 'Run3 (ms)': 1 },
	{ 'name': 'complex_index' }
);

var collection_sgemm_product_array = collection_forestfires.find(
	{ 'Run1 (ms)': { $gte: 90 }, 'Run2 (ms)': { $gte: 100 }, 'Run3 (ms)': { $lte: 100 } }
).toArray();
print(

	"Riješenje:"
	)
print(collection_sgemm_product_array.length);
print(


	)
print("8.Zadatak: Finished");

//==========================================================================================================================