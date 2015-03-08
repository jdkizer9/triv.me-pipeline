package com.trivme

//Spark imports
import org.apache.log4j.{Level, Logger}
// import org.apache.spark.{SparkContext, SparkConf}
// import org.apache.spark.rdd._
// import org.apache.spark.SparkContext._

import scala.io.Source

import scalaj.http._
import scala.collection.JavaConverters._

// import play.api.libs.json.Json
import play.api.libs.json._
import java.io.{PrintWriter, File}

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

object DBPedia {
    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val dataDirectory = args(0)
        val outputFileName = args(1)

        //read in person data 

        //val conf = new SparkConf().setAppName("trivme")
        //val sc = new SparkContext(conf)

        //val personData = sc.textFile(dataDirectory + "persondata_en.ttl") .

        // val selectedCategories = Set(  "<http://dbpedia.org/resource/Category:American_athletes>",
        //                         "<http://dbpedia.org/resource/Category:American_musicians>",
        //                         "<http://dbpedia.org/resource/Category:American_actors>")

        val selectedCategories = Set("<http://dbpedia.org/resource/Category:American_musicians>")

        val firstPassResources = Source.fromFile(dataDirectory +  "article_categories_en.ttl").getLines().
        filter( line => {
            val category = line.split(' ')(2)
            //println(resource)
            selectedCategories.contains(category)

        }) .
        map ( line => line.split(' ')(0).toString ).toSet
        // firstPassResources.foreach(println)
        println("First Pass Resources: " + firstPassResources.size)

        val personData = Source.fromFile(dataDirectory + "persondata_en.ttl").getLines() .
        filter( line => line.contains("<http://xmlns.com/foaf/0.1/name>")) .
        filter( line => {
            val resource = line.split(' ')(0).toString
            firstPassResources.contains(resource)
        }) .
        filter( line => line.contains("@en")) .
        map(line => {
                val nameResource = new scala.util.matching.Regex("""(<.*>) <.*> (.*)@en""", "resource", "name")
                
                val resourceNamePair: Option[(String, String)] = for {
                  nameResource(resource, name) <- nameResource findFirstIn line.toString
                } yield (resource.toString, name.toString)

                resourceNamePair match {
                    case Some((resource: String, nameWithQuotes: String)) => {
                        val name = nameWithQuotes.substring(1, nameWithQuotes.length-1)
                        (resource.toString, name.toString)
                    }
                    case None => (None, None)
                }
                
            }
            ).toMap

        println("Persons: " + personData.toList.length)

        // // val allPeopleResourceList = personData.map ({ case (resource, name) =>
        // //     resource
        // // }).collect.toList

        // personData.take(10).foreach(println)

        //val articleCategories = sc.textFile(dataDirectory + "article_categories_en.ttl").
        val articleCategoriesMap = Source.fromFile(dataDirectory +  "article_categories_en.ttl").getLines().
        filter( line => {
            val resource = line.split(' ')(0).toString
            //println(resource)
            firstPassResources.contains(resource)
        }).
        map(line => {
                val resourceCategory = new scala.util.matching.Regex("""(<.*>) <.*> (<.*>)""", "resource", "category")
                
                val resourceCategoryPair: Option[(String, String)] = for {
                  resourceCategory(resource, name) <- resourceCategory findFirstIn line
                } yield (resource, name)

                resourceCategoryPair match {
                    case Some((resource, category)) => {
                        (resource.toString, category.toString)
                    }
                    case None => (None, None)
                }
                
        }) .
        toList .
        groupBy( pair =>  pair._1 ) .
        mapValues ( l => l.map(pair => pair._2))



        // articleCategoriesMap.foreach(println)


        // articleCategoriesMap.take(10).foreach(println)
        println("Article Category Pairs: " + articleCategoriesMap.toList.length)


        val personDataWithCategories = personData.keys.
            filter(resource => articleCategoriesMap.contains(resource)).
            map( resource => {
            val categoryList = articleCategoriesMap(resource)
            val name = personData(resource)
            resource -> (name, categoryList)
            //(resource, (name, categoryList))
        }).toList.toMap

        // // personDataWithCategories.take(1).foreach(println)
        println("Person with category pairs: " + personDataWithCategories.toList.length)

        val personDataWithCategoriesAndPageViews = personDataWithCategories.keys.map( resource => {
            val name = personDataWithCategories(resource)._1
            //val response: HttpResponse[String] = Http("http://stats.grok.se/json/en/201503/" + name)
            // val response: HttpResponse[Map[String,String]] = Http("http://stats.grok.se/json/en/201503/" + name).
            // execute(parser = {inputStream =>
            //   Json.parse[Map[String,String]](inputStream)
            // })
            // if(response.isSuccess) {
            //     val body = response.body
            //     println(body)
            // }

            // {"daily_views": {"2015-03-01": 8134, "2015-03-03": 20084, "2015-03-02": 12203, "2015-03-05": 12337, "2015-03-04": 13822}, "project": "en", "month": "201503", "rank": 242, "title": "Abraham Lincoln"}
            
            //add try catch

            def never[T]: Future[T] = {
              val p = Promise[T]()
              p.future
            }

            def delay(t: Duration): Future[Unit] = async {
              try {
                blocking {
                  Await.ready(never[Unit], t)
                }
              } catch {
                case t: TimeoutException => {}
              }
            }

            delay(1 second)

            try { 
                val pageViews: HttpResponse[Int] = Http("http://stats.grok.se/json/en/201503/" + name).
                execute(parser = {is =>
                  val json:JsValue = Json.parse(Source.fromInputStream(is).map(_.toByte).toArray)
                  val dailyViews:JsValue = json.\("daily_views")
                  val dailyViewsMap: Map[String, Int] = dailyViews.as[Map[String, Int]]
                  dailyViewsMap.values.reduce(_ + _)
                })
                (resource, (personDataWithCategories(resource)._1, personDataWithCategories(resource)._2, pageViews.body))
            } catch {
               case _: Throwable => (resource, (personDataWithCategories(resource)._1, personDataWithCategories(resource)._2, 0))
            }

            

            

            // val categoryList = articleCategoriesMap.getOrElse(resource, None)
            // if(categoryList == None) None
            // else resource -> (personData(resource), categoryList)
        }).
        toList.
        sortWith( (pair1, pair2) => {
            pair1._2._3 < pair2._2._3
        }).
        reverse.
        take(500)

        val (finalResources, rest) = personDataWithCategoriesAndPageViews.unzip
        val finalResourcesSet = finalResources.toSet

        val bioMap = Source.fromFile(dataDirectory + "short_abstracts_en.ttl").getLines() .
        filter( line => {
            val resource = line.split(' ')(0).toString
            finalResourcesSet.contains(resource)
        }).
        map(line => {
            val abstractRegex = new scala.util.matching.Regex("""(<.*>) <.*> (.*)@en""", "resource", "articleAbstract")
            
            val resourceAbstractPair: Option[(String, String)] = for {
              abstractRegex(resource, articleAbstract) <- abstractRegex findFirstIn line.toString
            } yield (resource.toString, articleAbstract.toString)

            resourceAbstractPair match {
                case Some((resource: String, abstractWithoutQuotes: String)) => {
                    val abstractString = abstractWithoutQuotes.
                    substring(1, abstractWithoutQuotes.length-1)

                    //(resource.toString, abstractString.toString.replace(".", ""))
                    (resource.toString, abstractString.toString)
                }
                case None => (None, None)
            }

            
        }
        ).toMap

        bioMap.foreach(println)

        val wikiPageMap = Source.fromFile(dataDirectory + "wikipedia_links_en.ttl").getLines() .
        filter( line => {
            val resource = line.split(' ')(0).toString
            finalResourcesSet.contains(resource)
        }).
        map(line => {
            val wikiLinkRegex = new scala.util.matching.Regex("""(<.*>) <.*> (<.*>)""", "resource", "wikilink")
            
            val resourceWikilinkPair: Option[(String, String)] = for {
              wikiLinkRegex(resource, wikilink) <- wikiLinkRegex findFirstIn line.toString
            } yield (resource.toString, wikilink.toString)

            resourceWikilinkPair match {
                case Some((resource: String, wikilink: String)) => {
                    val wikilinkWithoutBrackets = wikilink.substring(1, wikilink.length-1)
                    (resource.toString, wikilinkWithoutBrackets.toString)
                }
                case None => (None, None)
            }
            
        }
        ).toMap

        wikiPageMap.foreach(println)

        val outputJsonArray: Array[JsValue] = personDataWithCategoriesAndPageViews.
        map( pair => {

            val dict = JsObject(Seq(
                "bio" -> JsString(bioMap(pair._1).toString),
                "name" -> JsString(pair._2._1.toString),
                "categories" -> JsArray(pair._2._2.map(v => JsString(v.toString))),
                "article_url" -> JsString(wikiPageMap(pair._1).toString)
            ))
            dict
        }).toArray

        val outputJson = JsObject(Seq("input" -> JsArray(outputJsonArray)))

        //println(Json.prettyPring(outputJson))

        val pw = new java.io.PrintWriter(new File(outputFileName))
        pw.write(Json.prettyPrint(outputJson))
        pw.close()












        
        





        
        






    }
}