package services.scalable.index.test

import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Commands, IndexBuilder, QueryableIndexDev}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class QueriesSpecDev extends Repeatable with Matchers {

  override val times: Int = 1

  "queries" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, 64)

    val indexId = UUID.randomUUID().toString

    val storage = new MemoryStorage()

    implicit val grpcIntIntSerializer = new GrpcByteSerializer[Int, Int]()
    implicit val grpcStringStringSerializer = new GrpcByteSerializer[String, String]()

    val ordering = ordString

    /*val builder = IndexBuilder.create[K, V](DefaultComparators.ordInt)
      .storage(storage)
      .serializer(DefaultSerializers.grpcIntIntSerializer)
      .keyToStringConverter(DefaultPrinters.intToStringPrinter)*/

    val builder = IndexBuilder.create[K, V](ordering)
      .storage(storage)
      .serializer(grpcStringStringSerializer)
      //.keyToStringConverter(DefaultPrinters.intToStringPrinter)

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES
    ))(storage, global), Duration.Inf).get

    var data = Seq.empty[(K, V, Boolean)]
    var index = new QueryableIndexDev[K, V](indexContext)(builder)

    val prefixes = (0 until 10).map{_ => RandomStringUtils.randomAlphabetic(3).toLowerCase}
      .distinct.toList

    def insert(): Unit = {

      val descriptorBackup = index.descriptor

      val n = 1000//rand.nextInt(20, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      val insertDup = rand.nextBoolean()

      /*for(i<-0 until n){

        rand.nextBoolean() match {
          case x if x && list.length > 0 && insertDup =>

            // Inserts some duplicate
            val (k, v, _) = list(rand.nextInt(0, list.length))
            list = list :+ (k, v, false)

          case _ =>

            //val k = rand.nextInt(1, 1000)//RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
            //val v = rand.nextInt(1, 1000)//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

            val k = s"${prefixes(rand.nextInt(0, prefixes.length))}${RandomStringUtils.randomAlphabetic(5)}".toLowerCase
            val v = RandomStringUtils.randomAlphabetic(5).toLowerCase


            if (!data.exists { case (k1, _, _) => ordering.equiv(k, k1) } &&
              !list.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
              list = list :+ (k, v, false)
            }
        }
      }*/

      //logger.debug(s"${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => new String(k)}}${Console.RESET}")

      list = Seq("pzxdhcap" -> "smsvu", "hotwkmak" -> "yakmk", "ievmbfgd" -> "ytadv", "hotpeuyv" -> "mfvnr",
        "zerqadlm" -> "kldyw", "bjhkdrtn" -> "kwtyp", "nmxirjoa" -> "mnbcp", "qeskydpw" -> "lwrmy",
        "lygojuxi" -> "alxon", "hotubhhf" -> "vzcwn", "lygsfwcx" -> "swxel", "nmxkpioq" -> "hmscu",
        "lwxnimiw" -> "yojgr", "lwxhqbnw" -> "ikrqa", "bjhyjhdh" -> "nwzrh", "lygeiqfb" -> "wniwx",
        "qesbbzey" -> "uhcuj", "zerervru" -> "azgrl", "fodnmcir" -> "nvnhz", "zercvslz" -> "gsmso",
        "fodmfbuf" -> "oqcyr", "nmxrnjgq" -> "sivgl", "lwxwraow" -> "zrjto", "pzxoyqbg" -> "zxxyn",
        "fodfzpwy" -> "acrrg", "qesbevzc" -> "sluro", "ievpgaub" -> "idcxn", "qesimyxj" -> "huahx",
        "zerasnst" -> "civhb", "hotkqhbx" -> "zibyz", "pzxhdvxc" -> "qslya", "qesivoho" -> "mchps",
        "lygjlusx" -> "kyneh", "hotcqbfz" -> "izitu", "fodowwxy" -> "chzzb", "lygsliml" -> "ntiuj",
        "lygihqsv" -> "fozxf", "lygynpjl" -> "xepdt", "bjhfcwok" -> "jguyx", "hotfvgze" -> "glypp",
        "bjhvbrhf" -> "hrgmp", "qeshhelf" -> "qanvl", "ievertds" -> "kzokw", "pzxsxsqx" -> "lylsu",
        "fodjcnep" -> "soebg", "lwxrclqk" -> "oxcse", "pzxbyfkf" -> "xzxna", "qesjhjgs" -> "mrqho",
        "lwxeoyzd" -> "oqvsr", "lwxsmzux" -> "zydpp", "lygkzlgq" -> "evuuv", "foddserl" -> "usxwx",
        "pzxiagdf" -> "iswsk", "nmxsztmx" -> "ivdjg", "zerfbsdq" -> "chihp", "bjhsokfa" -> "ukdnk",
        "lygxyrbi" -> "xwxob", "bjhioyse" -> "zjhue", "nmxqtpyj" -> "hdmdq", "nmxkfysp" -> "fpwmi",
        "bjheoyrf" -> "wflht", "ievbwjjk" -> "zkdcj", "bjhgdrgw" -> "vvllo", "lygbdklf" -> "umgzz",
        "bjhjwbmi" -> "poxgi", "lygfuhmo" -> "phdic", "nmxmyjwj" -> "fbcih", "ievdeelq" -> "wvrrf",
        "ievydede" -> "ukvmh", "lwxynktb" -> "knlvm", "qessuzbo" -> "snnwr", "ievmhejc" -> "ujtmd",
        "pzxzmbbr" -> "itulk", "bjhfuuih" -> "afjaj", "bjhjftlo" -> "wbtci", "zerfhedp" -> "pcklp",
        "fodmpwom" -> "uvkpv", "lygxqxxj" -> "fswiq", "nmxsqxva" -> "tzugp", "hotbfnty" -> "cxvid",
        "pzxwqhdh" -> "gnvay", "zeruirap" -> "fjynp", "fodxktjx" -> "glvfn", "lygwctbx" -> "yxmrb",
        "fodvdgtd" -> "kwjdy", "zerqgljk" -> "rscef", "ievmjpyl" -> "vsyss", "zerjqydf" -> "sgakd",
        "nmxwzowy" -> "dhlqz", "lyggkplt" -> "pgvgh", "lygjhjgl" -> "ucfgo", "lwxweyfo" -> "wdydm",
        "lygxgcfo" -> "wkpza", "hotanapo" -> "fhizh", "lygxwokl" -> "zwtws", "ievfznqs" -> "fpnix",
        "hotefhhe" -> "ucxzm", "pzxqlwwo" -> "mnbfn", "ievyuktg" -> "rdioo", "lygrloxb" -> "vymyr",
        "fodqvlhn" -> "ulixb", "qeslxkzd" -> "fvsxf", "lwxfjwqf" -> "uxezr", "ievpozka" -> "wgeth",
        "qeszyzbr" -> "vzekq", "bjhvnyxl" -> "oiobv", "qescqtip" -> "rgcvw", "zerojqts" -> "xisco",
        "fodicfuj" -> "yruiw", "pzxbcifo" -> "ppitv", "bjhdoahr" -> "echzv", "qespkkkb" -> "dhmnk",
        "nmxnykve" -> "sqlrd", "fodshvuc" -> "fwaol", "pzxdqjyf" -> "wzicd", "pzxzdacq" -> "qbseg",
        "bjhcwytg" -> "biqgd", "hotknchr" -> "rgreu", "lygkcqed" -> "gsfwv", "lygfgyfk" -> "qgnbf",
        "qesixmtl" -> "dourt", "pzxsnwnu" -> "hnuru", "lwxxmetg" -> "tkvol", "lwxafqrn" -> "vrlkc",
        "lwxyefoo" -> "oraex", "hotgxsnm" -> "jomym", "zerkgrwq" -> "agecc", "nmxcthyf" -> "ugjwm",
        "zerluxaj" -> "govtu", "qeskpgbn" -> "qliae", "lwxrzfak" -> "geoik", "nmxkvyif" -> "odqye",
        "lygjtwxg" -> "mzwgr", "bjhfrjep" -> "rccnu", "lyglljfs" -> "ryway", "ievsyssm" -> "uvbeo",
        "hotrpcgx" -> "eowpl", "pzxemmsl" -> "nkjxs", "hothozsh" -> "diwmj", "nmxaewpu" -> "pkilg",
        "lygtqlqt" -> "svpus", "nmxnhdzt" -> "ugaxs", "hotonwgi" -> "xitea", "qescrerm" -> "vgmeo",
        "ievzrlqy" -> "eqchx", "foddmdjj" -> "chytz", "nmxflqdi" -> "ppsxx", "hotfzplh" -> "qguil",
        "qeslwzrt" -> "bbeyk", "lygmuaer" -> "xlobf", "zernktng" -> "fecwa", "hotyhmjr" -> "gmtpi",
        "lwxcjacd" -> "bgldt", "bjhohbys" -> "sfpoq", "lygpaewk" -> "hqlax", "pzxytnat" -> "verxt",
        "pzxzyxsj" -> "xdool", "qespzxyn" -> "qklbq", "fodtqnub" -> "abygx", "nmxjrtuj" -> "jmoiy",
        "qesuawzk" -> "kdeyq", "pzxxvjov" -> "pviar", "bjhnqche" -> "mwvtp", "ievnbvyf" -> "rvsig",
        "qesfbxoc" -> "epqmb", "lwxoiqty" -> "tivpu", "bjhqytxm" -> "ksosy", "ievklrce" -> "rjbil",
        "hotajvzm" -> "gukjc", "nmxgktnm" -> "mxatb", "bjhtdrmz" -> "nzuxf", "pzxbsiyl" -> "phewj",
        "ievhnwah" -> "ortva", "qestngbh" -> "diweh", "ievagcbm" -> "raebd", "pzxytvby" -> "jcais",
        "lwxjeqpz" -> "lddtr", "zerhilzv" -> "liuge", "ievsqats" -> "rhfui", "bjhtskxr" -> "lorxt",
        "hotuljpw" -> "jrtaf", "hottpube" -> "zvchz", "pzxphybi" -> "sthpw", "nmxmpgbx" -> "cpswv",
        "zerzcwkj" -> "tigup", "nmxtylsh" -> "cjtgb", "lygnirkp" -> "vxdym", "hotourpy" -> "anpma",
        "hotejvld" -> "byumu", "nmxqfzxr" -> "rymhq", "zermyfgy" -> "jfdmc", "bjhligzi" -> "hcyxx",
        "bjhhnqij" -> "cgnmb", "ievyawhk" -> "agurm", "ievphlfm" -> "oylbe", "hotvrpis" -> "dxwax",
        "zerkxsqk" -> "jashq", "zersxqzs" -> "xvnnq", "ievfitbn" -> "nrgfm", "bjhkhjwr" -> "ilbhk",
        "nmxcgzgb" -> "jekgf", "hotjaftt" -> "pgyfr", "bjhhysqq" -> "unnev", "lygdumwn" -> "leqmu",
        "lwxidiei" -> "bbtve", "bjhlaage" -> "mquft", "lwxafjaw" -> "ogxsi", "ievkvfuv" -> "whxlw",
        "nmxdyymm" -> "dyigy", "pzxhuygo" -> "ivciq", "bjhonuit" -> "odhao", "lygoezpr" -> "gyjtn",
        "zeryttwz" -> "letdi", "nmxwavkq" -> "hbyyj", "lygetysp" -> "hqpkz", "zereprwq" -> "spxkk",
        "pzxathth" -> "ijxxu", "nmxdhfta" -> "tplpc", "lygyvvkn" -> "yslue", "qescgzgq" -> "hhoyq",
        "pzxjnsjx" -> "jwzrz", "bjhystov" -> "womic", "nmxlrztm" -> "pkdcy", "lwxihnlp" -> "zqbol",
        "hotvkuhd" -> "npauw", "fodpwrjg" -> "lpmpa", "lwxxvrak" -> "athsk", "lwxulhfj" -> "uvpst",
        "lwxtnbyo" -> "vwatf", "zerizwgx" -> "ulhwk", "hotasgml" -> "chfeg", "lwxsstla" -> "odvxh",
        "lygjxprx" -> "hxxcf", "foddsscs" -> "zouak", "ievlnjud" -> "ddqib", "qesksiws" -> "ezntf",
        "hotcwkhs" -> "sqxfu", "zermwyhy" -> "qudmb", "qeswlyvq" -> "rvdzv", "nmxgsgal" -> "mable",
        "nmxoetlm" -> "dmhjy", "nmxnwgzc" -> "qcfce", "hotsdvhy" -> "xeoyo", "nmxoxphh" -> "oatev",
        "ievyobmt" -> "dkkmw", "qesltzny" -> "mvfjv", "pzxltoul" -> "vtksc", "nmxepzhw" -> "qfppz",
        "zerjeojx" -> "qnqmq", "bjhetign" -> "ddzqz", "qesmwvci" -> "xzybo", "zeriyorb" -> "qycqp",
        "lwxgewsx" -> "lzjgp", "nmxgpllb" -> "zzisz", "hotddyie" -> "iaoff", "lygiwcjw" -> "jpyga",
        "pzxqvndf" -> "godxd", "qesxqufw" -> "ewbpx", "ievjstgq" -> "wqvqp", "ievnviti" -> "cbvsi",
        "qesjcvup" -> "rmlbf", "nmxyiteh" -> "hvwbc", "zerlqzkf" -> "jefgf", "lwxtfvml" -> "hzrjt",
        "hotkurhe" -> "xozzr", "fodxquvl" -> "peyaq", "pzxmcaqv" -> "tisdq", "zerofjcm" -> "arkpo",
        "ievyhhot" -> "igxtp", "zerobnrr" -> "ejwqz", "zericgtq" -> "oiizz", "pzxhsnld" -> "zcbio",
        "foddrpmw" -> "wtpdc", "qesooayh" -> "vlvwi", "lygboktc" -> "ftchk", "ievfjhut" -> "jgegy",
        "fodavgow" -> "ftlow", "zeraifak" -> "hzonw", "bjhjyufo" -> "kxifu", "ievzrydn" -> "qxsch", "fodwtrfl" -> "nkmwf", "fodtasek" -> "fphju", "lwxotydm" -> "wwqtq", "zergojrl" -> "wjmmm", "nmxcrrqx" -> "oxgda", "zerhyllr" -> "skpus", "lwxkivyn" -> "xyjag", "pzxtykaw" -> "bexzb", "fodclswg" -> "ljbtv", "lwxmqfht" -> "mhczc", "zermztvr" -> "sanyh", "nmxbkpjz" -> "czpqf", "lwxhhzpo" -> "zpkfp", "nmxjcogb" -> "szhzt", "zerzdlae" -> "tnpek", "ievipkou" -> "goicy", "hotomqgy" -> "uypcj", "ievziudg" -> "dswnb", "ievwdqgv" -> "gagfx", "fodamyrg" -> "dfvwz", "pzxeovoj" -> "epojc", "ievjzfrt" -> "bdtjr", "bjhjnall" -> "zhpuj", "bjhpkybi" -> "xqfer", "bjhayanb" -> "vocro", "lyggwvci" -> "aoxoq", "hothdjlt" -> "hcwes", "hotmptmf" -> "rkump", "qesytkvs" -> "jeeyl", "zermxtuy" -> "mwpkk", "hotlqvvf" -> "ygobk", "nmxikqyt" -> "bgvwh", "bjhfeooy" -> "xvata", "ievpmsra" -> "xonri", "pzxtnvhq" -> "litwx", "zernlkzf" -> "hwcje", "qesbuzrf" -> "rryxt", "qesednmv" -> "zdgir", "lygfdvyd" -> "lmmtk", "nmxretgz" -> "sastz", "fodwrzzu" -> "nlhle", "pzxcirit" -> "mpdms", "hotjcxle" -> "qggvw", "foddqlgl" -> "mrlnr", "nmxeqewg" -> "ffuxw", "lwxwbutv" -> "nbzsq", "hotluyst" -> "pvzsy", "ievxclmk" -> "gvgdw", "lyglvbgt" -> "vykjh", "bjhhzelv" -> "uziyh", "nmxnxywn" -> "uzinc", "hottzaeq" -> "vjfni", "fodbtlsz" -> "tvtba", "nmxrkyfv" -> "uvdnw", "zersrdkw" -> "tkuct", "hotcqhbe" -> "xpdbk", "zerenvam" -> "kgtqm", "ievgjmmc" -> "fmzws", "hotdqhms" -> "hubxs", "bjhqspiq" -> "myyve", "zerlhgby" -> "bvdcr", "lygwcbbu" -> "zgcfl", "hotzrgpy" -> "tennj", "pzxbibna" -> "zljol", "ievtaqof" -> "egsgn", "lwxeezjo" -> "lewwp", "nmxkdsmv" -> "qsebw", "lwxvjfdl" -> "hpfjv", "lygivfvu" -> "wlxrz", "bjhjrhzd" -> "sqrex", "zerlllfz" -> "mipho", "lwxvvuhe" -> "wsyrh", "lygehdzn" -> "yscoi", "lygemlwy" -> "ttkno", "nmxmpzix" -> "gzpig", "lygxwmmb" -> "dnafa", "zereswjh" -> "dztke", "bjhuxzux" -> "qkuwd", "zeremjak" -> "ywydj", "bjhzgqfd" -> "hhqot", "bjhcevpm" -> "ticgb", "qesuuqxl" -> "asvab", "lygsrctv" -> "ihkhg", "lwxigcpa" -> "xokex", "ievusmzh" -> "jfczy", "qesflxhy" -> "ivwwe", "lwxijfay" -> "qfcmc", "fodfqknk" -> "qijye", "ievtoeqs" -> "agccu", "nmxkcsey" -> "ccswt", "qeszjiua" -> "buxmj", "nmxkzroh" -> "pultt", "ievhgich" -> "ckbtv", "ievxwhug" -> "crckz", "lygquyao" -> "tgipp", "lwxqoznw" -> "czuah", "fodrzmlu" -> "wcuvr", "fodopnny" -> "rvbmr", "hotvwuul" -> "ndxlk", "hotiiida" -> "cwhig", "zerojkza" -> "mqccf", "lwxldwmd" -> "ocjgw", "lwxabmrj" -> "nynfr", "bjhguuti" -> "axvju", "hotkfahm" -> "gzdqh", "nmxexwki" -> "plkdw", "qeslicfy" -> "hzmqf", "pzxstwyn" -> "fccph", "fodzfstw" -> "wvhbj", "hotcwvsl" -> "emhds", "zerlapho" -> "dppkx", "qesfrydf" -> "htcdt", "fodlygjj" -> "yojri", "ievzzpor" -> "kcmet", "lwxckofc" -> "hpfxh", "fodtkrrr" -> "avuey", "qesnnflm" -> "lpjrg", "ievejopw" -> "knepi", "nmxrteui" -> "athii", "ievlktsk" -> "xfqmo", "fodowonv" -> "numiy", "ievzbouo" -> "kiisy", "zernctuj" -> "kklox", "qesgfgpg" -> "mguhc", "fodansqb" -> "hkbfa", "fodbiezl" -> "drfeu", "qescxlva" -> "bvupz", "pzxmiqdz" -> "kgwcr", "ievntblm" -> "vwjku", "hotmltqr" -> "ujqpx", "lwxpkner" -> "htkhz", "lwxkkdpm" -> "xvuwv", "lwxuycut" -> "lmolt", "qesfqtwr" -> "smqxv", "ievxdejq" -> "flbzl", "bjhrtbid" -> "hldiq", "fodlxvmw" -> "oqfju", "lygscwfk" -> "vbzgd", "pzxsmzfa" -> "giaqd", "qesyvvax" -> "wijgp", "lwxbaqwo" -> "xxpdk", "fodniydu" -> "pzuxs", "fodrdkbi" -> "ulbzf", "bjhiasrx" -> "xzbis", "zerugopg" -> "pxwus", "pzxrwkqt" -> "skede", "fodqgkyk" -> "lrwye", "fodaswev" -> "ehrou", "hotowmrk" -> "ntrud", "ievpsflk" -> "wtzuf", "zerkvqqp" -> "naqyg", "nmxkkenl" -> "vbrnm", "lwxpurko" -> "kiqch", "fodhlyid" -> "gbldu", "hotfvobg" -> "xinvi", "nmxlzfcd" -> "tsbmt", "ievenbth" -> "ovwzt", "fodrnpxr" -> "rzozg", "lwxrdzkp" -> "xwlyq", "bjhlfzms" -> "kgiit", "pzxwmtib" -> "knccy", "pzxmshzg" -> "jamxj", "nmxfwyiw" -> "cagxt", "qesgzsfs" -> "kvyft", "lygicwsp" -> "vdffw", "ievmqira" -> "cwemc", "pzxhxvjy" -> "xehgz", "hotnquxi" -> "uuhdy", "zervvgug" -> "pbxka", "lygvfgmm" -> "orflr", "lygwoevf" -> "fyavz", "ievvqbfr" -> "ozsbv", "qessmsxv" -> "ttaiz", "fodjkovy" -> "hlkmz", "nmxywkbf" -> "mmbtx", "nmxwnyck" -> "wrydf", "pzxqrtzc" -> "lrgry", "zeregsjt" -> "etxxh", "pzxozotl" -> "vhrzu", "ievbruvz" -> "fboaq", "pzxdgxpm" -> "hyffx", "lwxzktlh" -> "qhnbh", "qeshyrnq" -> "ungas", "pzxyigak" -> "yxdkm", "pzxlhqvp" -> "eagih", "hotcwuuc" -> "unpor", "fodrgtau" -> "dfrrv", "lygeoikk" -> "bsdli", "ievusvnx" -> "lghex", "lygdzome" -> "ucplu", "bjhzpndb" -> "dqjeb", "qesrbguu" -> "snukz", "qestsyna" -> "ymxum", "ievnmfbo" -> "iazrl", "pzxlgado" -> "bfpse", "pzxooasj" -> "bvgse", "nmxzkzwu" -> "ouojq", "fodnorcj" -> "rnvbb", "zernglze" -> "aktwc", "pzxrdjao" -> "qwfsf", "pzxjxtpb" -> "heysq", "ievuabxf" -> "wiheq", "hotjkmob" -> "fteuc", "bjhxpykq" -> "fruhi", "qeshfufi" -> "bdqsl", "pzxfcwcr" -> "hpgje", "qestwiiq" -> "ryadf", "nmxzkdyv" -> "ftpcx", "ievdaqtu" -> "uimmd", "lwxoihbf" -> "asgus", "qeszlczd" -> "ianus", "nmxydkeg" -> "llymc", "qesniiaz" -> "zpfgr", "zerpczsc" -> "axbec", "bjhlthja" -> "grwoh", "qesmiudd" -> "snqau", "ievjmgxl" -> "ndqny", "zerrixog" -> "ykblu", "nmxvdhfy" -> "geloz", "ievbfckb" -> "eqews", "qesahjhg" -> "fwkrd", "nmxhiyit" -> "qzjdx", "lygwksay" -> "wyxeq", "zerzzxld" -> "wpcqd", "fodyglfq" -> "bwhvg", "lygoyjio" -> "hriue", "lygsewpl" -> "gzdzs", "lygmzfhi" -> "qfeuy", "lygnekzv" -> "fvvom", "qesgigmz" -> "kjewx", "nmxizhbq" -> "jkqmg", "bjhkbifv" -> "mkzfx", "lwxesnzg" -> "lzugz", "pzxyyirm" -> "vucgj", "ieviwzvb" -> "hcxkh", "pzxzinsm" -> "tkjdh", "fodcsqge" -> "wkcbd", "bjhhcvna" -> "wczkk", "ievdrute" -> "sviwu", "lwxjxkvr" -> "tjrmi", "qesnrtzz" -> "cebeo", "zergqddn" -> "ydonk", "hotcsulu" -> "seqko", "bjhayjsj" -> "wqvsw", "lwxsuods" -> "dweiv", "ievhecab" -> "krmqy", "fodgfnfb" -> "obtvz", "ievfdmvb" -> "digfx", "zerieqdd" -> "kaiqf", "nmxixxoh" -> "xjvbe", "qesrwsra" -> "fpwdu", "lygnjurp" -> "cbtzf", "qesrregv" -> "qeqhu", "lwxgoxrc" -> "nnkny", "zerlkrnc" -> "wcujy", "fodrgsgk" -> "xfqgn", "nmxceump" -> "pmdfj", "nmxqrzzk" -> "duvsb", "hotvgxgm" -> "qfngx", "hotusqym" -> "tbfum", "pzxoenhc" -> "pbemk", "fodabilt" -> "vdssx", "zergqkww" -> "rccuf", "lygmyzzv" -> "ekace", "pzxxpcjw" -> "zxgik", "bjhwsppv" -> "frsss", "qesqjgob" -> "ykesd", "hotdulrk" -> "eulzt", "lwxhbiuf" -> "vfykc", "pzxguxzu" -> "mnavb", "pzxzjayt" -> "msobx", "qesjpypr" -> "pcxkb", "bjhqnuyk" -> "fclth", "nmxpntrj" -> "ylpki", "lwxrwixu" -> "wfzzj", "lygxtnrh" -> "nendm", "ievkmqeh" -> "tgema", "fodxpepq" -> "udyef", "ievciisl" -> "xzono", "bjhnmejo" -> "sysxh", "hotygage" -> "pnxww", "ievkjyse" -> "ksyyk", "nmxejsxh" -> "jzhqo", "fodjkeuy" -> "rlmlg", "bjhtrfxt" -> "ncqbj", "pzxtscdf" -> "upivk", "zerdemjs" -> "xmwom", "fodcmxiv" -> "ipzdw", "bjhfmjiv" -> "crpre", "hotewjzh" -> "sfidc", "zerutayl" -> "wanfc", "lygwzydf" -> "rolfg", "lwxdhfgs" -> "zyhrq", "nmxulufy" -> "aagdg", "ievdapaw" -> "ttojx", "qesvravr" -> "zhddr", "fodxonzp" -> "nhudn", "qesfgypn" -> "ndwrw", "hotpkncf" -> "rkiow", "nmxpywsi" -> "gefgj", "lwxatasa" -> "vmktp", "lyggjtbu" -> "fqokt", "hotxyadm" -> "gdbey", "ievitirl" -> "qkdts", "zerbyvjg" -> "yvlmx", "hotjfqpg" -> "hidrn", "pzxtpmkw" -> "wlvxe", "fodfcpkz" -> "ntxde", "hotjcurr" -> "viqyd", "nmxqizvl" -> "eicfg", "bjhqodhu" -> "cgvpw", "qesgzclc" -> "qtbxf", "zertkqdd" -> "jyyki", "bjhsoiff" -> "dhxrf", "lwxjcsex" -> "mxizx", "ievtfhpb" -> "xctiu", "ievnmflp" -> "ywnbn", "qesxjhkr" -> "pfozw", "zernuszh" -> "srump", "zerhnknk" -> "xqrdu", "lygtdxne" -> "ceftp", "zerwbhqq" -> "rieme", "nmxmhmkq" -> "peqel", "fodptdhj" -> "plqew", "hothxxzb" -> "pknjo", "bjhmeaef" -> "tiibl", "fodirkvw" -> "mmumy", "ieveykbf" -> "ohvlp", "zerkbwqv" -> "wmtjq", "zerexdhx" -> "stvcv", "lwxlxptf" -> "eyhfd", "hotvxftc" -> "iyirq", "fodezzsj" -> "epjey", "lwxgmful" -> "wihdk", "fodznipm" -> "zzrnk", "fodsjffm" -> "eqgrv", "bjhrarhh" -> "vytbl", "pzxjgusg" -> "tuiwy", "qesgilbx" -> "twwsz", "hotkvrya" -> "ekxfq", "qessusui" -> "wumse", "ievreawa" -> "srsot", "hotgxrgz" -> "uyxqb", "zerzjnxg" -> "qkqqj", "ievccmfo" -> "vzznv", "nmxwfwlv" -> "rgrhr", "nmxifeak" -> "tfxuq", "zercmnct" -> "xjtxf", "lygdutfc" -> "ncfla", "pzxyusee" -> "rjbny", "nmxolyum" -> "hasme", "lwxucikb" -> "ydwis", "pzxedxir" -> "szgqu", "pzxndsga" -> "kwgvz", "lygpmxcg" -> "jkweq", "fodczcys" -> "rysiq", "fodudbzw" -> "xnmir", "bjhfccjg" -> "xvyro", "nmxqizgz" -> "ntwfv", "lwxeraxd" -> "gkkbi", "fodxthqc" -> "suwuh", "zerldhrb" -> "zsvkz", "qeswmaoa" -> "pkliy", "hotimkck" -> "thfne", "ievbftvn" -> "ehesk", "nmxkfkst" -> "wumxo", "nmxcxshb" -> "fdaoh", "hotysifl" -> "muiso", "qesbpdox" -> "ceujn", "pzxhbals" -> "gpieb", "zerkipee" -> "mlske", "zergnpzl" -> "arizn", "pzxkdbsc" -> "ymwnq", "lwxsafpe" -> "traaa", "hottwugs" -> "lpapd", "qesdynyl" -> "euhom", "qesftmat" -> "usqtj", "fodnmnso" -> "blfgn", "pzxsobkc" -> "snuaa", "hothadpa" -> "phhzk", "nmxdvoxx" -> "yzudo", "bjhtiyjm" -> "cdwmu", "nmxntcjj" -> "ayexw", "nmxrfhtp" -> "vwnaz", "qesznwkc" -> "uqcqg", "hotqaxkw" -> "hvezu", "fodyuecb" -> "kmxft", "hotxyksw" -> "efvhy", "zersnitk" -> "gldgr", "lwxcjflg" -> "psmex", "pzxrmlfd" -> "dcaxj", "hotibrdl" -> "cvhuj", "ievafajr" -> "ffvpe", "fodhitio" -> "wbdhz", "bjhoimku" -> "kwftw", "lwxuudge" -> "wcoei", "qesktncs" -> "drdxw", "lwxonktz" -> "xdopi", "lygevijp" -> "wahmk", "qeseryqy" -> "myofs", "lygdxezi" -> "hmlwe", "pzxxzocs" -> "cwphf", "hotjfgrh" -> "tlaqg", "lygclzas" -> "tcmli", "qesmgkba" -> "ythro", "nmxtqiuw" -> "xlgpt", "lygetvcu" -> "cyvip", "ievaoclh" -> "gbnmn", "ievyvbjk" -> "gbogr", "bjhhnhfl" -> "lkzfi", "lyghvusb" -> "jvtkn", "zerrmuhq" -> "zubyt", "lygxhzut" -> "nayam", "ievjvcrj" -> "fyytj", "qesxcctb" -> "yrcbz", "hotpzfyk" -> "bfzrj", "fodvakwv" -> "nhpbt", "qesqopar" -> "lddxj", "bjhahrjc" -> "dowbb", "ievgowga" -> "phfmb", "qesjnxfo" -> "wiqli", "fodunoev" -> "xxuvv", "hotprxtw" -> "qxszr", "lygxecsx" -> "bsvfp", "fodscwuo" -> "lfgtm", "fodaoxug" -> "wnoys", "lwxbtbwv" -> "axefn", "zerokyrt" -> "qbcly", "lygltrfb" -> "viams", "bjhoggyi" -> "ewnjn", "pzxublnl" -> "mttyp", "pzxcjobv" -> "roitw", "pzxhklbg" -> "qtlta", "nmxfrhxf" -> "zjjny", "qesjrgrb" -> "ulyvy", "pzxmdraz" -> "yrxxu", "ievlpvtb" -> "rtfeb", "bjhqygre" -> "nqhil", "pzxoogjh" -> "kwvid", "hotqzxiw" -> "zyoir", "zereltyo" -> "yioxl", "pzxghhbs" -> "hoqcs", "ievulgij" -> "lusaa", "bjhbyiqs" -> "qwiit", "ievgeziv" -> "kyptb", "hotwyosf" -> "oxydg", "ievjqfuj" -> "zfkmo", "hotawevh" -> "kpcfw", "qesbsnlf" -> "lhsjz", "ievsnhkr" -> "ptziq", "lygzxfrm" -> "iosqs", "fodgjlbj" -> "mflyz", "fodxwonh" -> "yvhyt", "bjhxclwu" -> "lpbed", "nmxewcez" -> "exemg", "ievosdzi" -> "rtjgi", "zerktoxs" -> "blsha", "qesskrup" -> "hlfnn", "pzxmhuyw" -> "dtbii", "bjhfivvz" -> "hkuth", "pzxrfxsj" -> "vyizo", "nmxzxaar" -> "eouyw", "lwxqdtnn" -> "cvkcr", "ievxtmfe" -> "qzibm", "zerthcap" -> "mvskv", "hotjvxek" -> "clidw", "bjhkqxnc" -> "glgbb", "lwxbabvo" -> "hgwak", "nmxkgloh" -> "cvspw", "ievmmyko" -> "ozjbf", "pzxryngn" -> "vjfmw", "lwxzgbkl" -> "uacbp", "qesxpnjs" -> "ihtez", "hotzcczf" -> "iugau", "qeszedcn" -> "igsrn", "lwxejjtc" -> "adymr", "ievqxini" -> "dcozb", "zerypkvp" -> "zogtg", "bjhsqdms" -> "hjejl", "hotwzjhc" -> "qhlzy", "qeskwhnl" -> "hrswy", "qespvway" -> "yjgos", "fodrxmlx" -> "eruzq", "lwxdvqkg" -> "ezrrb", "bjhsejos" -> "skbib", "bjhjijjf" -> "hvgtr", "ievrckte" -> "sqlfz", "lygfhick" -> "ibqmn", "lygbliqa" -> "moodd", "nmxhgouj" -> "lrofo", "lwxecycb" -> "rxfqz", "bjhzirwt" -> "ycnri", "fodbgpxl" -> "icdat", "bjhhyqzc" -> "sugzx", "fodsjfkf" -> "mcqhn", "bjhjibfo" -> "umpbf", "ievnwbmg" -> "ysawx", "qesnxdxi" -> "bjveq", "pzxiuzrn" -> "jrofk", "fodvfyed" -> "ggevy", "pzxbpiap" -> "pexfn", "hothdedw" -> "xmqaf", "nmxghwsy" -> "mronp", "fodeuvnb" -> "xfsur", "qesagcyr" -> "rfupp", "qesnzjvq" -> "upotk", "bjhfnsml" -> "kugpc", "hotevcws" -> "mkvph", "zerlmfgm" -> "dmyps", "fodmopen" -> "gxtot", "ievehssi" -> "hqjaw", "lygpasng" -> "ctmle", "hotpstvq" -> "zyjoy", "hotcsatg" -> "gkuzb", "nmxkmfzi" -> "kglyv", "qesdpeae" -> "imimg", "lyggnvva" -> "njuqm", "bjhalnkp" -> "yngcb", "zerbkrcg" -> "najis", "lygzlydf" -> "gsuja", "qesunnrd" -> "tldba", "fodznlsy" -> "ucyry", "bjhywlwr" -> "zvthg", "zerdywzv" -> "tczpf", "qesqcwof" -> "tycyn", "fodduucd" -> "bmair", "pzxzcnpe" -> "rvpeu", "nmxmfckm" -> "ksdnj", "pzxuxttp" -> "eugrn", "ievnapqf" -> "bsinu", "lygbxtmg" -> "gieyg", "bjhgdmcz" -> "qwvlo", "bjhkqpjd" -> "fsldr", "hotpxykd" -> "kmkkm", "ievyeywj" -> "bvoyo", "qesiagtr" -> "jqgbp", "zervlygv" -> "bputo", "lygealtf" -> "ezgbu", "qeszukdb" -> "cyvmt", "hotdmzuc" -> "mwtmm", "zermosyw" -> "lctlz", "pzxpbhzp" -> "vlece", "bjhfloyd" -> "vlpki", "bjhgocrr" -> "wbusw", "qespyqhy" -> "dlcsg", "lyghlrxx" -> "hsyfp", "lwxlmzgh" -> "qnjdp", "lwxbdqgk" -> "qbmza", "pzxigita" -> "dzjek", "lygrgiwn" -> "iokiq", "nmxhkknm" -> "yxfvu", "bjhxveiz" -> "jjcsy", "zerumclm" -> "ycpsa", "fodpjfrn" -> "rlilu", "hotrpnzq" -> "uukux", "pzxdbvju" -> "hptxt", "hotwgxqt" -> "ilrov", "lwxqihfj" -> "xtgmj", "pzxhbvlf" -> "swfdr", "bjhmsepa" -> "xxjzv", "ievjulzb" -> "gucru", "fodcavvw" -> "nzytm", "hotprfjz" -> "pfhdw", "hotapfdr" -> "svhby", "bjhcavds" -> "acsff", "lwxvajrj" -> "strwd", "fodlqbdw" -> "qoxtx", "lwxeqqgc" -> "behsv", "qesxnhfl" -> "efrct", "lygrlytn" -> "rqetu", "lwxwneki" -> "uwfke", "zerddwky" -> "umnpm", "hotpdwrm" -> "uptjc", "nmxnazuw" -> "zfwwj", "lygnozkp" -> "qacjq", "pzxnftof" -> "dousl", "pzxvfdhi" -> "ypyog", "nmxgxqhz" -> "jviah", "zerawypj" -> "bzxia", "hotjieli" -> "kciry", "pzxazyox" -> "vfpny", "hothkpzf" -> "howmi", "nmxrwhpu" -> "cxnwa", "lygkrvyx" -> "peybd", "lygoyigf" -> "cjupr", "hotydwdw" -> "yqvjl", "bjhgmsxw" -> "svrps", "zerfgosf" -> "jipfm", "pzxvmvxf" -> "hdoaj", "hottbfms" -> "oucmy", "fodcsffm" -> "lvfkh", "lwxfutfq" -> "abaev", "ievjeobo" -> "tnfqw", "bjhdnmgo" -> "sljqs", "bjhrukih" -> "qelzf", "ievcshxd" -> "cdmal", "bjhqmntp" -> "sgqfc", "bjhifhfj" -> "dwpnk", "hotdcida" -> "liyxf", "ievspyer" -> "smxac", "lygzoqek" -> "utzvy", "bjhdtulp" -> "qshfq", "hotsghfe" -> "uxczf", "bjhmxqfk" -> "qijaj", "ievbqdym" -> "hygjv", "pzxxqwjm" -> "xuxop", "zerfmrxg" -> "icprt", "qesqcalg" -> "pggms", "bjhjbpmd" -> "qgfez", "nmxpeqix" -> "ceuki", "zerjcpmk" -> "zqfbd", "pzxzlyjj" -> "lozcb", "pzxnsgvi" -> "bkrlr", "pzxxhxgn" -> "vyqsn", "nmxeataj" -> "dlbff", "bjhcpjfx" -> "ukgzk", "bjhhooqi" -> "zmrnd", "pzxushsy" -> "fosvn", "lwxesmwu" -> "sqnvn", "ievaxegd" -> "vxzgg", "hoteafue" -> "varfi", "zerlytvd" -> "jrmbb", "lygvwxlt" -> "fecrg", "bjhmkjak" -> "jauao", "ievfoqkj" -> "poafj", "zercdekf" -> "xmppw", "nmxqxuap" -> "gsboj", "pzxqijfm" -> "slamt", "zeryegfq" -> "eksgb", "nmxwbkur" -> "dojny", "zerickba" -> "byzfd", "fodtearx" -> "tctbc", "qesjlles" -> "ouagq", "pzxpomki" -> "jpkrb", "pzxwrnyp" -> "vzssg", "lygmtreb" -> "giwvr", "pzxtbjnr" -> "iwxdn", "lwxgdboc" -> "qwkzp", "lygxmlim" -> "jxpzj", "lygkqxrd" -> "egydy", "ievljudk" -> "laeit", "fodliwof" -> "mcezm", "fodojapa" -> "mbnar", "fodqnybu" -> "bjvpi", "nmxfxitc" -> "dixoh", "qeslfdao" -> "ticjw", "qesocagk" -> "amlmu", "bjhevhcy" -> "lwrfk", "hoteqmuj" -> "bbxqq", "bjhykxkc" -> "mxoiz", "pzxlsmxi" -> "jamgs", "ievuvsvf" -> "mgyvt", "qesskznx" -> "ielud", "nmxggfmq" -> "oeroy", "pzxdyvnk" -> "qwhgv", "lygczqjf" -> "iabiy", "pzxmdwnr" -> "mlhft", "bjhhlmac" -> "oemms", "zerhdbhy" -> "nurfe", "hotxbnad" -> "wfoib", "zerrryio" -> "kyuji", "bjhgctfy" -> "gegkx", "hotoqlta" -> "htvmh", "qesmvhfn" -> "cbgfa", "hotyqlmv" -> "eatim", "bjhujxfe" -> "cekkm", "qeswhzwa" -> "oiqms", "bjhijwos" -> "ehehx", "lwxzzsmv" -> "hspjg", "lygrpptj" -> "ntpqj", "pzxdoplu" -> "ikvpd", "fodxpxpl" -> "incbb", "lygmfubx" -> "zzyqj", "bjhwacuo" -> "ommtd", "ievaglpw" -> "pkvuf", "hotkhwtm" -> "tfpiz", "fodhksfj" -> "vsqtm", "bjhgcgrj" -> "urzly", "fodtwqzt" -> "cvuli", "bjhjfcou" -> "lhupg", "pzxszuoy" -> "mcwxx", "nmxgujoi" -> "idhhr", "ievdspzo" -> "kuzox", "ievhjkzz" -> "quiwf", "bjhyokuy" -> "bxqct", "hotwudzz" -> "vsryx", "ievgtawe" -> "clkoj", "ievgfykz" -> "fodde", "bjhqkeet" -> "lphgs", "ievrfkhu" -> "txqxc", "zerqxxhj" -> "qlosk", "qeswusze" -> "znvyz", "qesuzyeh" -> "aakgi", "bjhftfst" -> "dllba", "lwxgoajd" -> "ufnsl", "hotxqhhj" -> "xtumr", "bjhzfyll" -> "tqycu", "fodtbmtx" -> "yjezc", "lygxecin" -> "vksix", "qesnctqc" -> "olmmv", "nmxhelle" -> "qrezj", "zeryndkm" -> "zjcju", "bjhigazt" -> "izvqq", "lwxtrijo" -> "hyewm", "pzxjjnhh" -> "tyxme", "lwxcfril" -> "cxvma", "lygmugxv" -> "doike", "fodgfmxi" -> "pzwtf", "lygdglbj" -> "yaudk", "zercfnvn" -> "wfhyj", "ievofddq" -> "tcmlm", "qesrvutp" -> "zrjlm", "nmxgefil" -> "lnpca", "foduiszw" -> "gxlmm", "hotqkogd" -> "eeqgy", "nmxamccj" -> "ggraz", "zerrgupl" -> "giqoj", "pzxkqamz" -> "skese", "fodxcmfo" -> "cyeyi", "lygczdje" -> "aosdb").map { x => (x._1, x._2, true)
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){
        logger.debug(s"${Console.GREEN_B}INSERTION OK: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexDev[K, V](newDescriptor)(builder)

        data = data ++ list

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      index = new QueryableIndexDev[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => Some(index.ctx.id)
        case false => Some(UUID.randomUUID.toString)
      }

      val descriptorBackup = index.descriptor

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, v, _) =>
        (k, RandomStringUtils.randomAlphabetic(5).toLowerCase, lastVersion)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){

        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map{case (k, _, _) => builder.ks(k)}}...${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexDev[K, V](newDescriptor)(builder)

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, true) }

        return
      }

      index = new QueryableIndexDev[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
      logger.debug(s"${Console.CYAN_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")
    }

    def remove(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => Some(index.ctx.id)
        case false => Some(UUID.randomUUID.toString)
      }

      val descriptorBackup = index.descriptor

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, _) =>
        (k, lastVersion)
      }

      val cmds = Seq(
        Commands.Remove[K, V](indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexDev[K, V](newDescriptor)(builder)

        logger.debug(s"${Console.YELLOW_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => ordering.equiv(k, k1) } }

        return
      }

      index = new QueryableIndexDev[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
    }

    val n = 1

    for(i<-0 until n){
      /*rand.nextInt(1, 4)*/1 match {
        case 1 => insert()
        case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty => remove()
        case _ => insert()
      }
    }

    val list = data
    data = data.sortBy(_._1)

    val dlist = data.map{case (k, v, _) => k -> v}
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))

    if(!data.isEmpty) {

      val suffixComp = new Ordering[K] {
        override def compare(k: K, term: K): Int = {
          val suffix = k.slice(3, k.length)

          ordering.compare(suffix, term)
        }
      }

      val inclusive = false//rand.nextBoolean()
      val pos = rand.nextInt(0, data.length)
      val (k, _, _) = data(962) //data(pos)
      val prefix = k.slice(0, 3)

      val term = k.slice(3, k.length)

      val prefixComp = new Ordering[K] {
        override def compare(x: K, prefix: K): Int = {
          val pk = x.slice(0, 3)
          ordering.compare(pk, prefix)
        }
      }

      val reverse = true//rand.nextBoolean()

      val idx2 = data.indexWhere(x => prefixComp.equiv(x._1, prefix) && (inclusive && suffixComp.gteq(x._1, term) || suffixComp.gt(x._1, term)))
      var slice2 = if (idx2 >= 0) data.slice(idx2, data.length).filter { x => prefixComp.equiv(x._1, prefix) && (inclusive && suffixComp.gteq(x._1, term) || suffixComp.gt(x._1, term)) }.map { x => x._1 -> x._2 }
        else Seq.empty[(K, V)]

      slice2 = if(reverse) slice2.reverse else slice2

      val itr = index.gt(prefix, k, inclusive, reverse)(prefixComp, ordering)
      val gtPrefixIndex = Await.result(TestHelper.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr = slice2 == gtPrefixIndex

      if(!cr){

        //index.prettyPrint()
        //logger.debug(s"""${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => s""""${builder.ks(k)}" -> "${builder.vs(v)}"""" }}${Console.RESET}""")

        println()
      }

      assert(cr)

      logger.info(Await.result(index.save(), Duration.Inf).toString)
      println()

    }

  }

}
